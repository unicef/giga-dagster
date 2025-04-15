from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as F,
    functions as f,
)
from pyspark.sql.functions import col
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

    df = s.createDataFrame(pdf)
    df = df.drop_duplicates()
    df = df.withColumn("date", f.to_date("timestamp"))
    df = df.withColumn(
        "gigasync_id",
        f.sha2(f.concat_ws("_", f.col("school_id_giga"), f.col("timestamp")), 256),
    )

    id_columns = [
        "country",
        "provider",
        "timestamp",
        "date",
        "school_id_govt",
        "school_id_giga",
        "gigasync_id",
    ]
    metric_columns = [col for col in df.columns if col not in id_columns]
    df = df.select([F.col(col).cast("STRING").alias(col) for col in df.columns])

    long_df = df.selectExpr(
        *id_columns,  # keep identifier columns
        f"stack({len(metric_columns)}, "
        + ", ".join([f"'{col}', {col}" for col in metric_columns])
        + ") as (metric_type, metric_value)",
    )
    column_actions = {
        "signature": f.sha2(f.concat_ws("|", *long_df.columns), 256),
    }
    long_df = long_df.withColumns(column_actions)

    context.log.info("table schema")
    context.log.info(long_df.schema.simpleString())

    return Output(
        long_df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(long_df),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def qos_availability_silver(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    qos_availability_raw: bytes,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session

    with BytesIO(qos_availability_raw) as buffer:
        buffer.seek(0)
        pdf = pd.read_csv(buffer)

    df = s.createDataFrame(pdf)
    df = df.drop_duplicates()

    df = df.withColumn("date", f.to_date("timestamp"))
    df = df.withColumn(
        "gigasync_id",
        f.sha2(f.concat_ws("_", f.col("school_id_giga"), f.col("timestamp")), 256),
    )

    # map column name
    # convert datatype
    # validate
    # all of this will come from where the schema is defined
    # can have a mapping of columns in availability with the datatype to begin with

    availability_columns = {
        "country": "string",
        "provider": "string",
        "timestamp": "timestamp",
        "query_time": "timestamp",
        "date": "string",
        "school_id_govt": "string",
        "school_id_giga": "string",
        "gigasync_id": "string",
        "metric_type": "string",
        "rta": "float",
        "lost": "float",
    }

    context.log.info(
        f"transform types schema columns before {df.schema.simpleString()}"
    )
    df = df.withColumns(
        {
            column_name: col(column_name).cast(
                availability_columns.get(column_name, "string")
            )
            for column_name in df.columns
        }
    )
    context.log.info(
        f"transform types after df with columns {df.schema.simpleString()}"
    )

    column_actions = {
        "signature": f.sha2(f.concat_ws("|", *df.columns), 256),
    }
    df = df.withColumns(column_actions)

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df),
        },
    )
