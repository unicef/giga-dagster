from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from numpy import nan
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as F,
)
from pyspark.sql.types import NullType
from src.resources import ResourceKey
from src.utils.adls import ADLSFileClient
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig

from dagster import (
    OpExecutionContext,
    Output,
    asset,
)


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def adhoc__load_health_master_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> Output[bytes]:
    raw = adls_file_client.download_raw(config.filepath)
    return Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__publish_health_master_to_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    health_master: bytes,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session

    with BytesIO(health_master) as buffer:
        buffer.seek(0)
        pdf = pd.read_csv(buffer)

    for col in pdf.select_dtypes(include="object").columns:
        pdf[col] = pdf[col].fillna("").astype(str).replace("", None)

    for col in pdf.select_dtypes(include=["number"]).columns:
        pdf[col] = pdf[col].astype(float).replace(nan, None)

    pdf = pdf.drop_duplicates()
    df = s.createDataFrame(pdf)

    context.log.info("original schema")
    context.log.info(df.schema.simpleString())
    void_columns = [
        field.name for field in df.schema.fields if isinstance(field.dataType, NullType)
    ]

    for col in void_columns:
        context.log.info(f"{col}")
        df = df.withColumn(col, F.col(col).cast("string"))

    context.log.info("updated schema")
    context.log.info(df.schema.simpleString())

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df),
        },
    )
