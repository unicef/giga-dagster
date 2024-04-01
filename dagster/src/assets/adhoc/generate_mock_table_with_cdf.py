from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark.sql import functions as f
from src.utils.delta import execute_query_with_error_handler
from src.utils.schema import get_schema_columns

from dagster import OpExecutionContext, asset

SOURCE_TABLE_NAME = "school_master.ben"
ZCDF_TABLE_NAME = "school_master.zcdf"


@asset
def adhoc__copy_original(
    context: OpExecutionContext,
    spark: PySparkResource,
):
    original = DeltaTable.forName(spark.spark_session, SOURCE_TABLE_NAME).toDF()
    columns = get_schema_columns(spark.spark_session, "school_master")

    query = (
        DeltaTable.createOrReplace(spark.spark_session)
        .tableName(ZCDF_TABLE_NAME)
        .addColumns(columns)
        .property("delta.enableChangeDataFeed", "true")
    )
    execute_query_with_error_handler(
        spark.spark_session, query, "school_master", "zcdf", context
    )

    (
        DeltaTable.forName(spark.spark_session, ZCDF_TABLE_NAME)
        .alias("source")
        .merge(original.alias("new"), "source.school_id_giga = new.school_id_giga")
        .whenNotMatchedInsertAll()
        .execute()
    )


@asset(deps=["adhoc__copy_original"])
def adhoc__generate_v2(spark: PySparkResource):
    master = DeltaTable.forName(spark.spark_session, ZCDF_TABLE_NAME).toDF()
    updates = master.withColumn(
        "cellular_coverage_availability",
        f.when(f.col("cellular_coverage_availability") == "No", f.lit("Yes")),
    )
    updates = updates.filter(f.col("cellular_coverage_availability") != "no_coverage")

    (
        DeltaTable.forName(spark.spark_session, ZCDF_TABLE_NAME)
        .alias("master")
        .merge(
            updates.alias("updates"), "master.school_id_giga = updates.school_id_giga"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


@asset(deps=["adhoc__generate_v2"])
def adhoc__generate_v3(spark: PySparkResource):
    master = DeltaTable.forName(spark.spark_session, ZCDF_TABLE_NAME).toDF()
    updates = master.withColumn(
        "cellular_coverage_availability",
        f.when(f.col("cellular_coverage_availability") == "Yes", f.lit("No")),
    )
    updates = updates.filter(f.col("cellular_coverage_type") != "2G")

    (
        DeltaTable.forName(spark.spark_session, ZCDF_TABLE_NAME)
        .alias("master")
        .merge(
            updates.alias("updates"), "master.school_id_giga = updates.school_id_giga"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
