import os

import pandas as pd
from delta import DeltaTable
from models import VALID_PRIMITIVES, Schema
from pyspark import sql
from pyspark.sql.functions import col, when
from src.utils.delta import (
    apply_physical_schema_changes,
    execute_query_with_error_handler,
)

from dagster import OpExecutionContext

PHYSICAL_SCHEMA_MAPPING: dict[str, list[str]] = {
    "school_geolocation": ["school_geolocation_staging", "school_geolocation_silver"],
    "school_master": ["school_master"],
    "school_coverage": ["school_coverage_staging", "school_coverage_silver"],
    "school_reference": ["school_reference"],
}

_SNAP_COLS = ["id", "name", "data_type", "is_nullable", "default_value"]


def get_filepath(context: OpExecutionContext) -> str:
    return context.run_tags["dagster/run_key"].split(":")[0]


def validate_raw_schema(
    context: OpExecutionContext,
    df: sql.DataFrame,
) -> sql.DataFrame:
    filepath = get_filepath(context)

    df = df.withColumn(
        "dq_invalid_data_type",
        when(col("data_type").isin(VALID_PRIMITIVES), 0).otherwise(1),
    )
    invalid_rows_df = df.filter(col("dq_invalid_data_type") == 1)
    if invalid_rows_df.count() > 0:
        invalid_values = [
            row["data_type"] for row in invalid_rows_df.select("data_type").collect()
        ]
        message = f"Invalid data type found in `{filepath}`. Valid values are {VALID_PRIMITIVES}, found {invalid_values}."
        context.log.error(message)
        raise ValueError(message)

    return df.drop("dq_invalid_data_type")


def save_schema_delta_table(context: OpExecutionContext, df: sql.DataFrame) -> None:
    filepath = get_filepath(context)
    spark = df.sparkSession

    filename = os.path.splitext(filepath.split("/")[-1])[0]
    schema_name = Schema.__schema_name__
    table_name = filename.replace("-", "_").lower()
    full_table_name = f"{schema_name}.{table_name}"
    columns = Schema.fields

    physical_schemas = PHYSICAL_SCHEMA_MAPPING.get(table_name, [])

    if not physical_schemas:
        # Original behaviour for uncovered schemas: wipe and reload
        query = (
            DeltaTable.createOrReplace(spark)
            .tableName(full_table_name)
            .addColumns(columns)
        )
        execute_query_with_error_handler(spark, query, schema_name, table_name, context)
        spark.catalog.refreshTable(full_table_name)
        (
            DeltaTable.forName(spark, full_table_name)
            .alias("master")
            .merge(df.alias("updates"), "master.name = updates.name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return

    # Covered schemas: diff-based update with physical DDL propagation

    # Step 1: Snapshot old metaschema state (before any changes)
    old_pdf = pd.DataFrame(columns=_SNAP_COLS)
    if spark.catalog.tableExists(full_table_name):
        old_pdf = spark.table(full_table_name).select(*_SNAP_COLS).toPandas()

    # Step 2: Compute full diff from old snapshot vs. incoming CSV DataFrame.
    #         Runs before the metaschema is updated so the old state is preserved on failure.
    incoming_pdf = df.select(*_SNAP_COLS).toPandas()
    merged = old_pdf.merge(incoming_pdf, on="id", suffixes=("_old", "_new"))

    renamed_mask = merged["name_old"] != merged["name_new"]
    renames: dict[str, str] = dict(
        zip(
            merged.loc[renamed_mask, "name_old"],
            merged.loc[renamed_mask, "name_new"],
            strict=False,
        )
    )

    type_mask = (merged["name_old"] == merged["name_new"]) & (
        merged["data_type_old"] != merged["data_type_new"]
    )
    type_changes: dict[str, str] = dict(
        zip(
            merged.loc[type_mask, "name_new"],
            merged.loc[type_mask, "data_type_new"],
            strict=False,
        )
    )

    null_mask = (merged["name_old"] == merged["name_new"]) & (
        merged["is_nullable_old"] != merged["is_nullable_new"]
    )
    nullability_changes: dict[str, bool] = dict(
        zip(
            merged.loc[null_mask, "name_new"],
            merged.loc[null_mask, "is_nullable_new"],
            strict=False,
        )
    )

    old_ids = set(old_pdf["id"])
    new_ids = set(incoming_pdf["id"])
    additions_pdf = incoming_pdf[~incoming_pdf["id"].isin(old_ids)]
    deletions: list[str] = old_pdf.loc[~old_pdf["id"].isin(new_ids), "name"].tolist()

    # Only require defaults when the schema already has rows — meaning physical tables
    # likely have data that would need backfilling. On a fresh first upload (old_pdf empty)
    # the physical tables are also empty, so no backfill is needed.
    if not old_pdf.empty:
        bad_additions = additions_pdf[
            (~additions_pdf["is_nullable"])
            & (
                additions_pdf["default_value"].isna()
                | (additions_pdf["default_value"].fillna("").str.strip() == "")
            )
        ]
        if not bad_additions.empty:
            bad = bad_additions["name"].tolist()
            message = f"New non-nullable columns must declare a default_value: {bad}"
            context.log.error(message)
            raise ValueError(message)

    # Step 3: Apply physical DDL first.
    #         If this raises, the metaschema is NOT updated — re-uploading the CSV retries safely.
    has_changes = (
        not additions_pdf.empty
        or renames
        or deletions
        or type_changes
        or nullability_changes
    )
    if has_changes:
        apply_physical_schema_changes(
            spark=spark,
            context=context,
            additions_pdf=additions_pdf,
            renames=renames,
            deletions=deletions,
            type_changes=type_changes,
            nullability_changes=nullability_changes,
            physical_schemas=physical_schemas,
        )

    # Step 4: Physical DDL succeeded — update the metaschema
    query = (
        DeltaTable.createIfNotExists(spark)
        .tableName(full_table_name)
        .addColumns(columns)
    )
    execute_query_with_error_handler(spark, query, schema_name, table_name, context)

    spark.catalog.refreshTable(full_table_name)
    (
        DeltaTable.forName(spark, full_table_name)
        .alias("master")
        .merge(df.alias("updates"), "master.id = updates.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
