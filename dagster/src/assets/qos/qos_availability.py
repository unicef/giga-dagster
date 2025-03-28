from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from numpy import nan
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import NullType
from pyspark.sql import functions as F
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
def qos_availability_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> Output[bytes]:
    raw = adls_file_client.download_raw(config.filepath)
    return Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def qos_availability_bronze(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    qos_availability_raw: bytes,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session

    with BytesIO(qos_availability_raw) as buffer:
        buffer.seek(0)
        pdf = pd.read_csv(buffer)

    column_actions = {
        "date": f.to_date(f.col("timestamp")),
    }

    df = s.createDataFrame(pdf)
    df = df.withColumns(column_actions).drop_duplicates()

    context.log.info("original schema")
    context.log.info(df.schema.simpleString())

    id_columns = ["country", "provider", "timestamp"]
    metric_columns = [col for col in df.columns if col not in id_columns]

    df = df.select([F.col(col).cast("STRING").alias(col) for col in df.columns])

    # stack the metric columns into a long format
    long_df = df.selectExpr(
        *id_columns,  # keep identifier columns
        f"stack({len(metric_columns)}, " + ", ".join([f"'{col}', {col}" for col in metric_columns]) + ") as (metric_type, metric_value)"
    )

    context.log.info("updated schema")
    context.log.info(long_df.schema.simpleString())

    return Output(
        long_df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(long_df),
        },
    )

