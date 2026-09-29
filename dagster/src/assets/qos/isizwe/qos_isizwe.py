import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.isizwe.common import aggregate_gold_dataframe, fetch_silver_dataframe
from src.custom.qos.isizwe.constants import COUNTRY_CODE
from src.custom.qos.isizwe.schema import ISIZWE_GOLD_SCHEMA
from src.custom.qos.schema_utils import enforce_schema, to_parquet_bytes
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext, Output, asset


@asset
def isizwe_qos(context: OpExecutionContext, spark: PySparkResource) -> Output:
    """Fetches yesterday's Zabbix history for Isizwe, aggregates hourly, and publishes
    gold/qos/ZAF. Combines process_isizwe_raw.py + aggregate_isizwe.py into a single
    in-memory run - the original split existed only to hand a file between two cron
    jobs, which Dagster doesn't need."""
    s: SparkSession = spark.spark_session
    today = dt.datetime.now().date().isoformat()

    silver_df = fetch_silver_dataframe(s, context)
    ADLSFileClient.upload_raw(
        None,
        to_parquet_bytes(silver_df),
        f"silver/qos/{COUNTRY_CODE}/isizwe_{today}.parquet",
    )
    context.log.info(f"{len(silver_df)} silver rows")

    gold_df = aggregate_gold_dataframe(silver_df)
    gold_df = enforce_schema(gold_df, ISIZWE_GOLD_SCHEMA)
    gold_parquet = to_parquet_bytes(gold_df)
    gold_filepath = f"gold/qos/{COUNTRY_CODE}/isizwe_{today}.parquet"
    ADLSFileClient.upload_raw(None, gold_parquet, gold_filepath)

    context.log.info(f"{len(gold_df)} gold rows -> {gold_filepath}")
    return Output(None, metadata={"rows": len(gold_df), "filepath": gold_filepath})
