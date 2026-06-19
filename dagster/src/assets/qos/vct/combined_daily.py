import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.vct.combined_daily import build_combined_dataframe
from src.custom.qos.vct.constants import COUNTRY_CODE
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def vct_qos(context: OpExecutionContext, spark: PySparkResource) -> Output:
    report_day = dt.date.today() - dt.timedelta(days=1)
    s: SparkSession = spark.spark_session
    context.log.info(f"Fetching VCT combined daily for {report_day}")

    combined_df = build_combined_dataframe(report_day, s, context)
    combined_df = combined_df.rename(
        columns={"serial": "device_id", "occurredAt": "timestamp"}
    )
    context.log.info(f"{len(combined_df)} event rows collected")

    csv_bytes = combined_df.to_csv(index=False).encode()
    filepath = (
        f"gold/qos/{COUNTRY_CODE}/vct_combined_daily_{report_day.isoformat()}.csv"
    )
    ADLSFileClient.upload_raw(None, csv_bytes, filepath)
    context.log.info(f"Uploaded {len(csv_bytes):,} bytes → {filepath}")

    return Output(None, metadata={"rows": len(combined_df), "filepath": filepath})
