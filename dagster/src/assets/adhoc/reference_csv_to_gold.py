from io import BytesIO

import numpy as np
import pandas as pd
import src.schemas
from dagster_pyspark import PySparkResource
from icecream import ic
from pyspark import sql
from pyspark.sql import SparkSession
from src.schemas import BaseSchema
from src.sensors.config import FileConfig
from src.utils.adls import ADLSFileClient, get_output_filepath

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
    spark: PySparkResource,
    config: FileConfig,
    adhoc__load_reference_csv: bytes,
) -> sql.DataFrame:
    schema_name = ic(config.metastore_schema)
    schema_class: BaseSchema = getattr(src.schemas, schema_name)
    spark_schema = ic(schema_class.schema)
    s: SparkSession = spark.spark_session

    buffer = BytesIO(adhoc__load_reference_csv)
    buffer.seek(0)
    df = pd.read_csv(buffer)
    df = df.fillna(np.nan).replace([np.nan], [None])
    gold = s.createDataFrame(df, schema=spark_schema)
    yield Output(gold, metadata={"filepath": get_output_filepath(context)})
