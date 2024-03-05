from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import SparkSession
from src.sensors.base import FileConfig
from src.utils.adls import ADLSFileClient, get_output_filepath

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def adhoc__load_qos_bra_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    yield Output(raw, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_v2_io_manager")
def adhoc__publish_qos_bra_to_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    adhoc__load_qos_bra_csv: bytes,
) -> sql.DataFrame:
    s: SparkSession = spark.spark_session

    buffer = BytesIO(adhoc__load_qos_bra_csv)
    buffer.seek(0)
    df = pd.read_csv(buffer)
    sdf = s.createDataFrame(df)
    yield Output(sdf, metadata={"filepath": get_output_filepath(context)})
