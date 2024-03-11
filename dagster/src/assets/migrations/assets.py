import os

from dagster_pyspark import PySparkResource
from delta import DeltaTable
from models import Schema
from pyspark.sql import SparkSession
from src.utils.adls import ADLSFileClient
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, asset


@asset
@capture_op_exceptions
def initialize_metaschema(_: OpExecutionContext, spark: PySparkResource) -> None:
    s: SparkSession = spark.spark_session
    schema_name = Schema.__schema_name__
    s.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`").show()


@asset(deps=["initialize_metaschema"])
@capture_op_exceptions
def migrate_schema(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> None:
    s: SparkSession = spark.spark_session
    pdf = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"]
    )
    df = s.createDataFrame(pdf, schema=Schema.schema)

    schema_name = Schema.__schema_name__
    filename = os.path.splitext(context.run_tags["dagster/run_key"].split("/")[-1])[0]
    table_name = filename.replace("-", "_").lower()
    full_table_name = f"{schema_name}.{table_name}"

    columns = Schema.fields
    (
        DeltaTable.createOrReplace(s)
        .tableName(full_table_name)
        .addColumns(columns)
        .execute()
    )

    (
        DeltaTable.forName(s, full_table_name)
        .alias("master")
        .merge(df.alias("updates"), "master.name = updates.name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    if s.catalog.isCached(full_table_name):
        s.catalog.refreshTable(full_table_name)
    else:
        s.catalog.cacheTable(full_table_name)
