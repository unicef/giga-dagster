from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.settings import settings
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, asset


@asset
@capture_op_exceptions
def migrate_schema(
    _: OpExecutionContext,
    spark: PySparkResource,
):
    s: SparkSession = spark.spark_session
    s.read.csv(f"{settings.AZURE_BLOB_CONNECTION_URI}/raw_schema/schema.csv")
