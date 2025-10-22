from io import BytesIO
import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from numpy import nan
from pyspark.sql import (
    SparkSession,
    functions as F,
    functions as f,
)
from pyspark.sql.types import NullType
from src.resources import ResourceKey
from src.utils.adls import ADLSFileClient
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.spark import transform_types

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


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_availability_transforms(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    qos_availability_raw: bytes,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session

    with BytesIO(qos_availability_raw) as buffer:
        buffer.seek(0)
        df = pd.read_csv(buffer).fillna(nan).replace([nan], [None])

    sdf = s.createDataFrame(df)
    column_actions = {
        "signature": f.sha2(f.concat_ws("|", *sdf.columns), 256),
        "gigasync_id": f.sha2(
            f.concat_ws(
                "_",
                f.col("school_id_giga"),
                f.col("timestamp"),
            ),
            256,
        ),
        "date": f.to_date(f.col("timestamp")),
    }
    sdf = sdf.withColumns(column_actions).drop_duplicates(["gigasync_id"])
    df = sdf.toPandas()

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def publish_qos_availability_to_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    qos_availability_transforms: sql.DataFrame,
) -> Output[sql.DataFrame]:
    df = transform_types(
        qos_availability_transforms,
        config.metastore_schema,
        context,
    )

    context.log.info("original schema")
    context.log.info(df.schema.simpleString())

    void_columns = [
        field.name for field in df.schema.fields if isinstance(field.dataType, NullType)
    ]

    for col in void_columns:
        context.log.info(f"{col}")
        df = df.withColumn(col, f.col(col).cast("string"))

    context.log.info("updated schema")
    context.log.info(df.schema.simpleString())

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df),
        },
    )
