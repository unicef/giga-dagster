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
from src.utils.adls import ADLSFileClient, get_output_filepath
from src.utils.spark import transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def adhoc__load_qos_bra_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    yield Output(raw, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__qos_bra_transforms(
    context: OpExecutionContext,
    spark: PySparkResource,
    adhoc__load_qos_bra_csv: bytes,
) -> pd.DataFrame:
    s: SparkSession = spark.spark_session

    buffer = BytesIO(adhoc__load_qos_bra_csv)
    buffer.seek(0)
    df = pd.read_csv(buffer).fillna(nan).replace([nan], [None])

    sdf = s.createDataFrame(df)
    column_actions = {
        "gigasync_id": f.sha2(
            f.concat(
                f.col("school_id_giga"),
                f.lit("_"),
                f.col("timestamp"),
            ),
            256,
        ),
        "date": f.to_date(f.col("timestamp")),
    }
    sdf = sdf.withColumns(column_actions)

    yield Output(sdf.toPandas(), metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_v2_io_manager")
def adhoc__publish_qos_bra_to_gold(
    context: OpExecutionContext,
    adhoc__qos_bra_transforms: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
    df_transformed = transform_types(
        adhoc__qos_bra_transforms, config.metastore_schema, context
    )
    yield Output(df_transformed, metadata={"filepath": get_output_filepath(context)})
