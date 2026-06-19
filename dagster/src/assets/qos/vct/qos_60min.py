import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.vct.constants import COUNTRY_CODE
from src.custom.qos.vct.qos_60min import build_snapshot_dataframe
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def vct_qos_raw(context: OpExecutionContext, spark: PySparkResource) -> Output:
    now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    window_end = now.replace(minute=0)
    window_start = window_end - dt.timedelta(hours=1)

    s: SparkSession = spark.spark_session
    context.log.info(f"Fetching VCT 60-min QoS for {window_start} → {window_end}")

    snapshot_df = build_snapshot_dataframe(s, context, window_start, window_end)
    snapshot_df = snapshot_df.rename(
        columns={"serial": "device_id", "window_start": "timestamp"}
    )
    context.log.info(f"{len(snapshot_df)} APs in snapshot")

    stamp = window_end.strftime("%Y%m%dT%H%M%SZ")
    csv_bytes = snapshot_df.to_csv(index=False).encode()
    filepath = f"gold/qos-raw/{COUNTRY_CODE}/vct_qos_raw_60min_{stamp}.csv"
    ADLSFileClient.upload_raw(None, csv_bytes, filepath)
    context.log.info(f"Uploaded {len(csv_bytes):,} bytes → {filepath}")

    return Output(None, metadata={"rows": len(snapshot_df), "filepath": filepath})
