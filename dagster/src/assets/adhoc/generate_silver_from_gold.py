from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StructType
from src.constants import DataTier, constants
from src.spark.transform_functions import add_missing_columns
from src.utils.delta import check_table_exists, execute_query_with_error_handler
from src.utils.metadata import get_table_preview
from src.utils.op_config import DatasetConfig
from src.utils.schema import get_schema_columns
from src.utils.sentry import capture_op_exceptions
from src.utils.spark import compute_row_hash, transform_types

from dagster import OpExecutionContext, Output, asset


@asset
@capture_op_exceptions
def adhoc__generate_silver_geolocation_from_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DatasetConfig,
) -> Output[None]:
    s: SparkSession = spark.spark_session
    table_name = config.country_code.lower()
    schema_columns = get_schema_columns(s, "school_geolocation")

    master = (
        DeltaTable.forName(s, f"school_master.{table_name}").toDF().drop("signature")
    )
    master = add_missing_columns(master, get_schema_columns(s, "school_master"))

    if check_table_exists(s, "school_reference", table_name, DataTier.GOLD):
        reference = (
            DeltaTable.forName(s, f"school_reference.{table_name}")
            .toDF()
            .drop("signature")
        )
        reference = add_missing_columns(
            reference, get_schema_columns(s, "school_reference")
        )
    else:
        reference_schema_columns = get_schema_columns(s, "school_reference")
        reference = s.createDataFrame([], StructType(reference_schema_columns)).drop(
            "signature"
        )

    gold = master.alias("master").join(
        reference.alias("reference"), on="school_id_giga", how="left"
    )
    silver = gold.select([c.name for c in schema_columns if c.name != "signature"])
    silver = silver.withColumns(
        {
            "education_level_govt": f.coalesce(
                f.col("education_level_govt"), f.lit("Unknown")
            ),
            "school_id_govt_type": f.coalesce(
                f.col("education_level_govt"), f.lit("Unknown")
            ),
        }
    )
    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, "school_geolocation", context)
    silver = compute_row_hash(silver, context)

    out_table_name = f"school_geolocation_silver.{table_name}"

    create_query = (
        DeltaTable.createIfNotExists(s)
        .tableName(out_table_name)
        .addColumns(schema_columns)
        .property("delta.enableChangeDataFeed", "true")
        .property(
            "delta.logRetentionDuration", constants.school_master_retention_period
        )
    )

    execute_query_with_error_handler(
        s,
        create_query,
        "school_geolocation_silver",
        config.country_code,
        context,
    )

    (
        DeltaTable.forName(s, out_table_name)
        .alias("source")
        .merge(silver.alias("new"), "source.school_id_giga = new.school_id_giga")
        .whenNotMatchedInsertAll()
        .execute()
    )

    return Output(
        None,
        metadata={
            "count": silver.count(),
            "preview": get_table_preview(silver),
        },
    )


@asset
@capture_op_exceptions
def adhoc__generate_silver_coverage_from_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: DatasetConfig,
) -> Output[None]:
    s: SparkSession = spark.spark_session
    table_name = config.country_code.lower()
    schema_columns = get_schema_columns(s, "school_coverage")

    master = (
        DeltaTable.forName(s, f"school_master.{table_name}").toDF().drop("signature")
    )
    master = add_missing_columns(master, get_schema_columns(s, "school_master"))

    if check_table_exists(s, "school_reference", table_name, DataTier.GOLD):
        reference = (
            DeltaTable.forName(s, f"school_reference.{table_name}")
            .toDF()
            .drop("signature")
        )
        reference = add_missing_columns(
            reference, get_schema_columns(s, "school_reference")
        )
    else:
        reference_schema_columns = get_schema_columns(s, "school_reference")
        reference = s.createDataFrame([], StructType(reference_schema_columns)).drop(
            "signature"
        )

    gold = master.alias("master").join(
        reference.alias("reference"), on="school_id_giga", how="left"
    )
    silver = gold.select([c.name for c in schema_columns if c.name != "signature"])
    silver = silver.withColumns(
        {
            "cellular_coverage_availability": f.coalesce(
                f.col("cellular_coverage_availability"), f.lit("Unknown")
            ),
            "cellular_coverage_type": f.coalesce(
                f.col("cellular_coverage_type"), f.lit("Unknown")
            ),
        }
    )
    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, "school_coverage", context)
    silver = compute_row_hash(silver, context)

    out_table_name = f"school_coverage_silver.{table_name}"

    create_query = (
        DeltaTable.createIfNotExists(s)
        .tableName(out_table_name)
        .addColumns(schema_columns)
        .property("delta.enableChangeDataFeed", "true")
        .property(
            "delta.logRetentionDuration", constants.school_master_retention_period
        )
    )

    execute_query_with_error_handler(
        s,
        create_query,
        "school_coverage_silver",
        config.country_code,
        context,
    )

    (
        DeltaTable.forName(s, out_table_name)
        .alias("source")
        .merge(silver.alias("new"), "source.school_id_giga = new.school_id_giga")
        .whenNotMatchedInsertAll()
        .execute()
    )

    return Output(
        None,
        metadata={
            "count": silver.count(),
            "preview": get_table_preview(silver),
        },
    )
