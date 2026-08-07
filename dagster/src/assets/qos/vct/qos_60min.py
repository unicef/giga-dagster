from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.vct.constants import COUNTRY_CODE
from src.custom.qos.vct.qos_60min import build_snapshot_dataframe
from src.custom.qos.vct.schema import QOS_60MIN_SCHEMA, enforce_schema, to_parquet_bytes
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def vct_qos_raw(context: OpExecutionContext, spark: PySparkResource) -> Output:
    s: SparkSession = spark.spark_session

    snapshot_df, _, window_end = build_snapshot_dataframe(s, context)
    snapshot_df = snapshot_df.rename(
        columns={"serial": "device_id", "window_start": "timestamp"}
    )
    snapshot_df = enforce_schema(snapshot_df, QOS_60MIN_SCHEMA)
    context.log.info(f"{len(snapshot_df)} APs in snapshot")

    stamp = window_end.strftime("%Y%m%dT%H%M%SZ")
    parquet_bytes = to_parquet_bytes(snapshot_df)
    filepath = f"gold/qos-raw/{COUNTRY_CODE}/vct_qos_raw_60min_{stamp}.parquet"
    ADLSFileClient.upload_raw(None, parquet_bytes, filepath)
    context.log.info(f"Uploaded {len(parquet_bytes):,} bytes → {filepath}")

    return Output(None, metadata={"rows": len(snapshot_df), "filepath": filepath})
