from dagster_pyspark import PySparkResource
from pyspark import sql
from src.utils.adls import ADLSFileClient, get_output_filepath

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_delta_io_manager")
def qos_csv_to_gold(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    df = adls_file_client.download_csv_as_spark_dataframe(
        context.run_tags["dagster/run_key"], spark.spark_session
    )
    yield Output(df, metadata={"filepath": get_output_filepath(context)})
