from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import NullType
from src.sensors.config import FileConfig
from src.utils.adhoc.rename_columns import REFERENCE_COLUMNS_TO_ADD
from src.utils.adls import ADLSFileClient, get_output_filepath
from src.utils.spark import transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def adhoc__load_reference_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    yield Output(raw, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_v2_io_manager")
def adhoc__publish_reference_to_gold(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    adhoc__load_reference_csv: bytes,
) -> sql.DataFrame:
    s: SparkSession = spark.spark_session

    buffer = BytesIO(adhoc__load_reference_csv)
    buffer.seek(0)
    df = pd.read_csv(buffer)
    sdf = s.createDataFrame(df)
    columns_to_add = {
        column: lit(None).cast(NullType()) for column in REFERENCE_COLUMNS_TO_ADD
    }
    sdf = sdf.withColumns(columns_to_add)

    gold = transform_types(sdf, config.metastore_schema, context)
    yield Output(gold, metadata={"filepath": get_output_filepath(context)})
