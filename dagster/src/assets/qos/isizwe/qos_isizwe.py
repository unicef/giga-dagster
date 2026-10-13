import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.isizwe.common import (
    aggregate_gold_dataframe,
    fetch_silver_dataframe,
)
from src.custom.qos.isizwe.constants import COUNTRY_CODE
from src.custom.qos.isizwe.schema import ISIZWE_GOLD_SCHEMA
from src.custom.qos.schema_utils import enforce_prd_schema, to_parquet_bytes
from src.utils.adls import ADLSFileClient

from dagster import DailyPartitionsDefinition, OpExecutionContext, Output, asset

# Adjust to Isizwe's actual go-live date before relying on backfills before this.
ISIZWE_QOS_START_DATE = "2024-01-01"


@asset(partitions_def=DailyPartitionsDefinition(start_date=ISIZWE_QOS_START_DATE))
def isizwe_qos(context: OpExecutionContext, spark: PySparkResource) -> Output:
    """Fetches one day's Zabbix history for Isizwe, aggregates hourly, and publishes
    gold/qos/ZAF. Combines process_isizwe_raw.py + aggregate_isizwe.py into a single
    in-memory run - the original split existed only to hand a file between two cron
    jobs, which Dagster doesn't need. The partition key is the completed day being
    queried - a live run targets yesterday, a backfill run targets whichever past
    day. (The original script named its output files after "today" while actually
    querying "yesterday"'s history - fixed here so the filename matches the data.)"""
    s: SparkSession = spark.spark_session
    target_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    silver_df = fetch_silver_dataframe(target_date, s, context)
    if len(silver_df) == 0:
        context.log.warning(f"No Isizwe data for {target_date}")
        return Output(None, metadata={"rows": 0})

    ADLSFileClient.upload_raw(
        None,
        to_parquet_bytes(silver_df),
        f"silver/qos/{COUNTRY_CODE}/isizwe_{target_date.isoformat()}.parquet",
    )
    context.log.info(f"{len(silver_df)} silver rows")

    gold_df = aggregate_gold_dataframe(silver_df)
    gold_df = enforce_prd_schema(gold_df, ISIZWE_GOLD_SCHEMA)
    gold_parquet = to_parquet_bytes(gold_df)
    gold_filepath = f"gold/qos/{COUNTRY_CODE}/isizwe_{target_date.isoformat()}.parquet"
    ADLSFileClient.upload_raw(None, gold_parquet, gold_filepath)

    context.log.info(f"{len(gold_df)} gold rows -> {gold_filepath}")
    return Output(None, metadata={"rows": len(gold_df), "filepath": gold_filepath})
