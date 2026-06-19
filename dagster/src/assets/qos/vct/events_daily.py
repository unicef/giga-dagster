import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.vct.constants import COUNTRY_CODE
from src.custom.qos.vct.events_daily import build_events_dataframe
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def vct_qos_availability(context: OpExecutionContext, spark: PySparkResource) -> Output:
    report_day = dt.date.today() - dt.timedelta(days=1)
    s: SparkSession = spark.spark_session
    context.log.info(f"Fetching VCT events for {report_day}")

    events_df = build_events_dataframe(report_day, s, context)
    events_df = events_df.rename(
        columns={"serial": "device_id", "occurredAt": "timestamp"}
    )
    context.log.info(f"{len(events_df)} events collected")

    csv_bytes = events_df.to_csv(index=False).encode()
    filepath = f"raw/qos-availability/{COUNTRY_CODE}/vct_qos_availability_{report_day.isoformat()}.csv"
    ADLSFileClient.upload_raw(None, csv_bytes, filepath)
    context.log.info(f"Uploaded {len(csv_bytes):,} bytes → {filepath}")

    return Output(None, metadata={"rows": len(events_df), "filepath": filepath})
