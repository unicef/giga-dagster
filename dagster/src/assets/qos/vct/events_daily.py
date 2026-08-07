import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.vct.constants import COUNTRY_CODE
from src.custom.qos.vct.events_daily import build_events_dataframe
from src.custom.qos.vct.schema import (
    QOS_AVAILABILITY_SCHEMA,
    enforce_schema,
    to_parquet_bytes,
)
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def vct_qos_availability(context: OpExecutionContext, spark: PySparkResource) -> Output:
    report_day = dt.date.today() - dt.timedelta(days=1)
    s: SparkSession = spark.spark_session
    context.log.info(f"Fetching VCT events for {report_day}")

    events_df = build_events_dataframe(report_day, s, context)
    events_df = enforce_schema(events_df, QOS_AVAILABILITY_SCHEMA)
    context.log.info(f"{len(events_df)} events collected")

    parquet_bytes = to_parquet_bytes(events_df)
    filepath = f"raw/qos-availability/{COUNTRY_CODE}/vct_qos_availability_{report_day.isoformat()}.parquet"
    ADLSFileClient.upload_raw(None, parquet_bytes, filepath)
    context.log.info(f"Uploaded {len(parquet_bytes):,} bytes → {filepath}")

    return Output(None, metadata={"rows": len(events_df), "filepath": filepath})
