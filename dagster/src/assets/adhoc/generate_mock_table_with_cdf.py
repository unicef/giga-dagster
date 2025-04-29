from delta import DeltaTable
from pyspark.sql import functions as f
from src.resources import ResourceKey
from src.utils.delta import execute_query_with_error_handler
from src.utils.schema import get_schema_columns
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, asset

SOURCE_TABLE_NAME = "school_master.ben"
ZCDF_TABLE_NAME = "school_master.zcdf"


@asset(required_resource_keys={ResourceKey.SPARK.value})
@capture_op_exceptions
def adhoc__copy_original(
    context: OpExecutionContext,
):
    s = context.resources.spark.spark_session
    original = DeltaTable.forName(s, SOURCE_TABLE_NAME).toDF()
    columns = get_schema_columns(s, "school_master")

    query = (
        DeltaTable.createOrReplace(s)
        .tableName(ZCDF_TABLE_NAME)
        .addColumns(columns)
        .property("delta.enableChangeDataFeed", "true")
    )
    execute_query_with_error_handler(
        s,
        query,
        "school_master",
        "zcdf",
        context,
    )

    (
        DeltaTable.forName(s, ZCDF_TABLE_NAME)
        .alias("source")
        .merge(original.alias("new"), "source.school_id_giga = new.school_id_giga")
        .whenNotMatchedInsertAll()
        .execute()
    )


@asset(deps=["adhoc__copy_original"], required_resource_keys={ResourceKey.SPARK.value})
@capture_op_exceptions
def adhoc__generate_v2(
    context: OpExecutionContext,
):
    s = context.resources.spark.spark_session
    master = DeltaTable.forName(s, ZCDF_TABLE_NAME).toDF()
    updates = master.withColumn(
        "cellular_coverage_availability",
        f.when(f.col("cellular_coverage_availability") == "No", f.lit("Yes")),
    )
    updates = updates.filter(f.col("cellular_coverage_availability") != "no_coverage")

    (
        DeltaTable.forName(s, ZCDF_TABLE_NAME)
        .alias("master")
        .merge(
            updates.alias("updates"),
            "master.school_id_giga = updates.school_id_giga",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


@asset(deps=["adhoc__generate_v2"], required_resource_keys={ResourceKey.SPARK.value})
@capture_op_exceptions
def adhoc__generate_v3(
    context: OpExecutionContext,
):
    s = context.resources.spark.spark_session
    master = DeltaTable.forName(s, ZCDF_TABLE_NAME).toDF()
    updates = master.withColumn(
        "cellular_coverage_availability",
        f.when(f.col("cellular_coverage_availability") == "Yes", f.lit("No")),
    )
    updates = updates.filter(f.col("cellular_coverage_type") != "2G")

    (
        DeltaTable.forName(s, ZCDF_TABLE_NAME)
        .alias("master")
        .merge(
            updates.alias("updates"),
            "master.school_id_giga = updates.school_id_giga",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
