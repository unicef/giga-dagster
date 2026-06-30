import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.vct.combined_daily import build_combined_dataframe
from src.custom.qos.vct.constants import COUNTRY_CODE
from src.custom.qos.vct.schema import (
    QOS_COMBINED_SCHEMA,
    enforce_schema,
    to_parquet_bytes,
)
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def vct_qos(context: OpExecutionContext, spark: PySparkResource) -> Output:
    report_day = dt.date.today() - dt.timedelta(days=1)
    s: SparkSession = spark.spark_session
    context.log.info(f"Fetching VCT combined daily for {report_day}")

    combined_df = build_combined_dataframe(report_day, s, context)
    combined_df = enforce_schema(combined_df, QOS_COMBINED_SCHEMA)
    context.log.info(f"{len(combined_df)} event rows collected")

    parquet_bytes = to_parquet_bytes(combined_df)
    filepath = (
        f"gold/qos/{COUNTRY_CODE}/vct_combined_daily_{report_day.isoformat()}.parquet"
    )
    ADLSFileClient.upload_raw(None, parquet_bytes, filepath)
    context.log.info(f"Uploaded {len(parquet_bytes):,} bytes → {filepath}")

    return Output(None, metadata={"rows": len(combined_df), "filepath": filepath})
