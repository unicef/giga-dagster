import os

from dagster_pyspark import PySparkResource
from models import Schema
from pyspark.sql import SparkSession
from src.utils.adls import ADLSFileClient
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, asset

from .core import save_schema_delta_table, validate_raw_schema


@asset
@capture_op_exceptions
def initialize_metaschema(_: OpExecutionContext, spark: PySparkResource) -> None:
    s: SparkSession = spark.spark_session
    schema_name = Schema.__schema_name__
    s.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")


@asset(deps=["initialize_metaschema"])
@capture_op_exceptions
def migrate_schema(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> None:
    s: SparkSession = spark.spark_session
    filepath = context.run_tags["dagster/run_key"]
    pdf = adls_file_client.download_csv_as_pandas_dataframe(filepath)

    filename = os.path.splitext(filepath.split("/")[-1])[0]
    schema_name = Schema.__schema_name__
    table_name = filename.replace("-", "_").lower()
    full_table_name = f"{schema_name}.{table_name}"
    df = s.createDataFrame(pdf, schema=Schema.schema)
    df = validate_raw_schema(context, df)
    save_schema_delta_table(context, df)

    if s.catalog.isCached(full_table_name):
        s.catalog.refreshTable(full_table_name)
    else:
        s.catalog.cacheTable(full_table_name)
