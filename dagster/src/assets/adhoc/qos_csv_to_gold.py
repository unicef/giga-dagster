from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from numpy import nan
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from src.sensors.base import FileConfig
from src.utils.adls import ADLSFileClient
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.spark import transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def adhoc__load_qos_csv(
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    yield Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__qos_transforms(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    adhoc__load_qos_csv: bytes,
) -> pd.DataFrame:
    s: SparkSession = spark.spark_session

    with BytesIO(adhoc__load_qos_csv) as buffer:
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
    sdf = sdf.withColumns(column_actions).dropDuplicates(["gigasync_id"])
    context.log.info(f"Calculated SHA256 signature for {sdf.count()} rows")

    df_pandas = sdf.toPandas()
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key="adls_delta_v2_io_manager")
def adhoc__publish_qos_to_gold(
    context: OpExecutionContext,
    adhoc__qos_transforms: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
    df_transformed = transform_types(
        adhoc__qos_transforms, config.metastore_schema, context
    )
    yield Output(
        df_transformed,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_transformed),
        },
    )
