import pandas as pd
from delta.tables import DeltaMergeBuilder, DeltaTable, DeltaTableBuilder
from icecream import ic
from pyspark import sql
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import DataType, StructField, StructType

from dagster import InputContext, OpExecutionContext, OutputContext
from src.constants import DataTier
from src.exceptions import MutexException
from src.settings import settings
from src.utils.schema import construct_full_table_name, construct_schema_name_for_tier


def execute_query_with_error_handler(
    spark: SparkSession,
    query: DeltaTableBuilder | DeltaMergeBuilder,
    schema_name: str,
    table_name: str,
    context: InputContext | OutputContext | OpExecutionContext,
) -> None:
    full_table_name = f"{schema_name}.{table_name}"

    try:
        query.execute()
    except AnalysisException as exc:
        if "DELTA_TABLE_NOT_FOUND" in str(exc):
            # This error gets raised when you delete the Delta Table in ADLS and subsequently try to re-ingest the
            # same table. Its corresponding entry in the metastore needs to be dropped first.
            #
            # Deleting a table in ADLS does not drop its metastore entry; the inverse is also true.
            context.log.warning(
                f"Attempting to drop metastore entry for `{full_table_name}`...",
            )
            spark.sql(f"DROP TABLE `{schema_name}`.`{table_name.lower()}`")
            query.execute()
            context.log.info("ok")
        else:
            raise exc


def create_delta_table(
    spark: SparkSession,
    schema_name: str,
    table_name: str,
    columns: StructType | list[StructField],
    context: InputContext | OutputContext | OpExecutionContext,
    *,
    if_not_exists: bool = False,
    replace: bool = False,
    partition_by: list[str] | None = None,
) -> None:
    if if_not_exists and replace:
        raise MutexException(
            "Only one of `if_not_exists` or `replace` can be set to True.",
        )

    full_table_name = construct_full_table_name(schema_name, table_name)
    create_stmt = DeltaTable.create

    if if_not_exists:
        create_stmt = DeltaTable.createIfNotExists
    if replace:
        create_stmt = DeltaTable.createOrReplace

    query = (
        create_stmt(spark)
        .tableName(full_table_name)
        .addColumns(columns)
        .property("delta.enableChangeDataFeed", "true")
        .property("delta.columnMapping.mode", "name")
    )
    if partition_by:
        query = query.partitionedBy(*partition_by)
    execute_query_with_error_handler(spark, query, schema_name, table_name, context)


def create_schema(spark: SparkSession, schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")


def build_deduped_merge_query(
    master: DeltaTable,
    updates: sql.DataFrame,
    primary_key: str,
    update_columns: list[str],
    *,
    context: OpExecutionContext | OutputContext = None,
    is_partial_dataset=False,
    is_qos=False,
) -> DeltaMergeBuilder | None:
    """
    Delta Lake increments the dataset version and generates a change log when performing
    a merge, regardless of whether there were actually any changes. We perform a basic
    signature check to determine if there were actually any changes and perform the
    relevant merge operations only if there is at least one change for that particular
    operation (i.e. insert, update, delete).

    IMPORTANT: Because of this, it is crucial that you do not pass in partial datasets for the
    `updates` DataFrame without explicitly setting the `is_partial_dataset` flag. Otherwise,
    you may end up deleting more rows than you intended.
    """
    incoming = updates.alias("incoming")

    if is_qos:
        incoming_partitions = [
            r.date for r in incoming.select(f.col("date")).distinct().collect()
        ]

        if context is not None:
            context.log.info(
                f"Processing {len(incoming_partitions)} date partitions: {incoming_partitions}"
            )

        partition_filter = f.col("date").isin(incoming_partitions)

        master_df = master.toDF().filter(partition_filter)
        merge_condition = f.col(f"master.{primary_key}") == f.col(
            f"incoming.{primary_key}"
        )

        bc_incoming_partitions = incoming.sparkSession.sparkContext.broadcast(
            incoming_partitions
        )
        merge_condition = merge_condition & f.col("master.date").isin(
            bc_incoming_partitions.value
        )
    else:
        master_df = master.toDF()
        merge_condition = f.col(f"master.{primary_key}") == f.col(
            f"incoming.{primary_key}"
        )

    master_ids = master_df.select(
        primary_key, f.col("signature").alias("master_signature")
    )
    incoming_ids = incoming.select(
        primary_key, f.col("signature").alias("incoming_signature")
    )

    updates_df = incoming_ids.join(master_ids, primary_key, "inner")
    inserts_df = incoming_ids.join(master_ids, primary_key, "left_anti")
    deletes_df = master_df.select(primary_key).join(
        incoming_ids, primary_key, "left_anti"
    )

    has_updates = (
        updates_df.filter(f.col("master_signature") != f.col("incoming_signature"))
        .limit(1)
        .count()
    ) > 0
    inserts_count = inserts_df.count()
    deletes_count = deletes_df.count()

    has_insertions = inserts_count > 0
    has_deletions = deletes_count > 0

    if not any(
        [has_updates, has_insertions, (has_deletions and not is_partial_dataset)]
    ):
        return None

    if context is not None:
        context.log.info(f"{is_qos=}, {is_partial_dataset=}")
        context.log.info(f"{inserts_count=}, {deletes_count=}, {has_updates=}")

    query = master.alias("master").merge(incoming.alias("incoming"), merge_condition)

    if has_updates:
        query = query.whenMatchedUpdate(
            "master.signature <> incoming.signature",
            dict(
                zip(
                    update_columns,
                    [f"incoming.{c}" for c in update_columns],
                    strict=True,
                ),
            ),
        )
    if has_insertions:
        query = query.whenNotMatchedInsertAll()
    if ic(has_deletions) and not is_partial_dataset:
        query = query.whenNotMatchedBySourceDelete()

    return query


def build_deduped_delete_query(
    master: DeltaTable,
    delete_ids: list[str],
    primary_key: str,
) -> DeltaMergeBuilder | None:
    master_df = master.toDF()

    deletes_df = master_df.filter(master_df[primary_key].isin(delete_ids))

    has_deletes = deletes_df.count() > 0

    if not (ic(has_deletes)):
        return None

    query = master.alias("master").merge(
        deletes_df.alias("deletes"),
        f"master.{primary_key} = deletes.{primary_key}",
    )

    if has_deletes:
        query = query.whenMatchedDelete()

    return query


def get_change_operation_counts(df: sql.DataFrame):
    counts = df.groupBy("_change_type").agg(f.count("*").alias("count")).collect()

    def get_count_by_change_type(change_type: str):
        return next((c["count"] for c in counts if c["_change_type"] == change_type), 0)

    return {
        "added": get_count_by_change_type("insert"),
        "modified": get_count_by_change_type("update_postimage"),
        "deleted": get_count_by_change_type("delete"),
    }


def check_table_exists(
    spark: SparkSession, schema_name: str, table_name: str, data_tier: DataTier = None
) -> bool:
    tiered_schema_name = construct_schema_name_for_tier(
        schema_name,
        data_tier,
    )
    table_path = ic(
        f"{settings.SPARK_WAREHOUSE_DIR}/{tiered_schema_name}.db/{table_name.lower()}"
    )

    return ic(spark.catalog.tableExists(table_name, tiered_schema_name)) and ic(
        DeltaTable.isDeltaTable(spark, table_path)
    )


def get_changed_datatypes(
    context: OpExecutionContext, existing_schema: StructType, updated_schema: StructType
) -> dict[str, DataType]:
    original_datatypes = {}
    changed_datatypes = {}
    context.log.info(f"Existing schema {existing_schema}")
    context.log.info(f"Updated schema {updated_schema}")

    for column in existing_schema:
        if (
            match_ := next((c for c in updated_schema if c.name == column.name), None)
        ) is not None:
            if match_.dataType != column.dataType:
                changed_datatypes[match_.name] = match_.dataType
                original_datatypes[match_.name] = column.dataType

    context.log.info(f"Original datatypes: {original_datatypes}")
    context.log.info(f"Changed datatypes: {changed_datatypes}")
    return changed_datatypes


def build_nullability_queries(
    context: OpExecutionContext,
    existing_schema: StructType,
    updated_schema: StructType,
    table_name: str,
) -> list[str]:
    context.log.info("Building nullability queries...")
    context.log.info(f"Existing schema {existing_schema}")
    context.log.info(f"Updated schema {updated_schema}")
    alter_stmts = []
    for column in existing_schema:
        if (
            match_ := next((c for c in updated_schema if c.name == column.name), None)
        ) is not None:
            if match_.nullable != column.nullable:
                alter_stmts.append(
                    f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {column.name}_not_null"
                )

                if match_.nullable:
                    alter_stmts.append(
                        f"ALTER TABLE {table_name} ALTER COLUMN {column.name} DROP NOT NULL"
                    )

                else:
                    alter_stmts.append(
                        f"ALTER TABLE {table_name} ADD CONSTRAINT {column.name}_not_null CHECK ({column.name} is not null)"
                    )

    return alter_stmts


def apply_physical_schema_changes(
    spark: SparkSession,
    context: OpExecutionContext,
    additions_pdf: pd.DataFrame,
    renames: dict[str, str],
    deletions: list[str],
    type_changes: dict[str, str],
    nullability_changes: dict[str, bool],
    physical_schemas: list[str],
) -> None:
    for schema_name in physical_schemas:
        if not spark.catalog.databaseExists(schema_name):
            context.log.info(f"Schema '{schema_name}' does not exist yet — skipping")
            continue

        for table_row in spark.sql(f"SHOW TABLES IN `{schema_name}`").collect():
            tbl = table_row.tableName
            full = f"`{schema_name}`.`{tbl}`"
            try:
                existing = {field.name for field in spark.table(full).schema}
            except AnalysisException as exc:
                if "DELTA_TABLE_NOT_FOUND" in str(exc):
                    context.log.warning(
                        f"[SKIP] {full}: table files missing in storage — skipping"
                    )
                    continue
                raise

            if renames or deletions:
                _ensure_column_mapping_enabled(spark, full, context)

            _apply_additions(spark, context, full, existing, additions_pdf)
            _apply_renames(spark, context, full, existing, renames)
            _apply_deletions(spark, context, full, existing, deletions)
            _apply_type_changes(spark, context, full, existing, type_changes)
            _apply_nullability_changes(
                spark, context, full, existing, nullability_changes
            )


def _apply_additions(
    spark: SparkSession,
    context: OpExecutionContext,
    full: str,
    existing: set[str],
    additions_pdf: pd.DataFrame,
) -> None:
    for _, row in additions_pdf.iterrows():
        col_name = row["name"]
        type_sql = _data_type_to_sql(row["data_type"])
        default = row.get("default_value")
        if col_name in existing:
            context.log.info(f"[ADD] {full}: '{col_name}' already exists — skipping")
            continue
        has_default = pd.notna(default) and str(default).strip()
        safe_default = str(default).replace("'", "''") if has_default else None
        if safe_default:
            spark.sql(
                f"ALTER TABLE {full} ADD COLUMNS (`{col_name}` {type_sql} DEFAULT '{safe_default}')"  # nosec B608
            )
            spark.sql(
                f"UPDATE {full} SET `{col_name}` = '{safe_default}' WHERE `{col_name}` IS NULL"  # nosec B608
            )
        else:
            spark.sql(f"ALTER TABLE {full} ADD COLUMNS (`{col_name}` {type_sql})")
        context.log.info(f"[ADD] {full}: +{col_name} ({type_sql})")


def _apply_renames(
    spark: SparkSession,
    context: OpExecutionContext,
    full: str,
    existing: set[str],
    renames: dict[str, str],
) -> None:
    for old_name, new_name in renames.items():
        if old_name in existing:
            spark.sql(f"ALTER TABLE {full} RENAME COLUMN `{old_name}` TO `{new_name}`")
            context.log.info(f"[RENAME] {full}: {old_name} → {new_name}")
        elif new_name not in existing:
            context.log.warning(
                f"[RENAME] {full}: neither '{old_name}' nor '{new_name}' found — skipping"
            )


def _apply_deletions(
    spark: SparkSession,
    context: OpExecutionContext,
    full: str,
    existing: set[str],
    deletions: list[str],
) -> None:
    for col_name in deletions:
        if col_name in existing:
            spark.sql(f"ALTER TABLE {full} DROP COLUMN `{col_name}`")
            context.log.info(f"[DROP] {full}: -{col_name}")


def _apply_type_changes(
    spark: SparkSession,
    context: OpExecutionContext,
    full: str,
    existing: set[str],
    type_changes: dict[str, str],
) -> None:
    if not type_changes:
        return
    existing_df = spark.table(full)
    updated_df = existing_df
    for col_name, new_type in type_changes.items():
        if col_name in existing:
            updated_df = updated_df.withColumn(
                col_name, existing_df[col_name].cast(_data_type_to_sql(new_type))
            )
    (
        updated_df.write.option("overwriteSchema", "true")
        .format("delta")
        .mode("overwrite")
        .saveAsTable(full.replace("`", ""))
    )
    context.log.info(f"[TYPE CHANGE] {full}: {type_changes}")


def _apply_nullability_changes(
    spark: SparkSession,
    context: OpExecutionContext,
    full: str,
    existing: set[str],
    nullability_changes: dict[str, bool],
) -> None:
    for col_name, new_nullable in nullability_changes.items():
        if col_name not in existing:
            continue
        if new_nullable:
            spark.sql(
                f"ALTER TABLE {full} DROP CONSTRAINT IF EXISTS {col_name}_not_null"
            )
            spark.sql(f"ALTER TABLE {full} ALTER COLUMN `{col_name}` DROP NOT NULL")
        else:
            try:
                spark.sql(f"ALTER TABLE {full} ALTER COLUMN `{col_name}` SET NOT NULL")
                spark.sql(
                    f"ALTER TABLE {full} ADD CONSTRAINT {col_name}_not_null "
                    f"CHECK (`{col_name}` IS NOT NULL)"
                )
            except AnalysisException as exc:
                if "DELTA_CONSTRAINT_ALREADY_EXISTS" not in str(exc):
                    raise
        context.log.info(
            f"[NULLABILITY] {full}: {col_name} → {'nullable' if new_nullable else 'NOT NULL'}"
        )


def _data_type_to_sql(data_type: str) -> str:
    return {
        "string": "STRING",
        "integer": "INT",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "timestamp": "TIMESTAMP",
        "boolean": "BOOLEAN",
    }[data_type.lower()]


def _ensure_column_mapping_enabled(
    spark: SparkSession,
    full_table: str,
    context: OpExecutionContext,
) -> None:
    props = {
        r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {full_table}").collect()
    }
    if props.get("delta.columnMapping.mode") == "name":
        return
    context.log.info(f"Enabling column mapping on {full_table}")
    spark.sql(
        f"ALTER TABLE {full_table} SET TBLPROPERTIES ("
        f"'delta.minReaderVersion' = '2', "
        f"'delta.minWriterVersion' = '5', "
        f"'delta.columnMapping.mode' = 'name')"
    )
