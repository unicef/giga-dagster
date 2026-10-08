import datetime as dt

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.custom.qos.mongolia.common import (
    aggregate_gold_dataframe,
    build_raw_clean_dataframe,
)
from src.custom.qos.mongolia.schema import (
    MONGOLIA_GOLD_SCHEMA,
    MONGOLIA_RAW_CLEAN_SCHEMA,
)
from src.custom.qos.schema_utils import enforce_prd_schema, to_parquet_bytes
from src.utils.adls import ADLSFileClient

from dagster import DailyPartitionsDefinition, OpExecutionContext, Output, asset

# Mongolia's device poller (mongolia_qos_raw_json) has landed data since roughly
# late 2024 - adjust to the actual go-live date before relying on backfills before
# this, and note that any date with no raw snapshots under raw/qos/MNG/<date>/
# will just produce zero rows, same as backfill_mongolia_raw.py's WARNING-and-skip.
MONGOLIA_QOS_GOLD_START_DATE = "2024-11-01"


@asset(
    partitions_def=DailyPartitionsDefinition(start_date=MONGOLIA_QOS_GOLD_START_DATE)
)
def mongolia_qos_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    adls_file_client: ADLSFileClient,
) -> Output:
    """Cleans one day's raw device snapshots and aggregates them hourly, publishing
    both gold/qos-raw/MNG and gold/qos/MNG. Combines process_mongolia_raw.py +
    process_mongolia_clean.py into a single in-memory run - the original two-hour gap
    between them existed only to let a file land on blob storage before the next cron
    fired, which Dagster doesn't need. process_bandwidth_utilization.py's independent
    staging-only pipeline is intentionally not ported (see schema.py). The partition
    key is the completed day being cleaned - a live run targets yesterday, a backfill
    run targets whichever past day (needs mongolia_qos_raw_json to have actually
    polled that day - see module docstring)."""
    s: SparkSession = spark.spark_session
    query_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    raw_clean_df = build_raw_clean_dataframe(query_date, adls_file_client, s, context)
    if len(raw_clean_df) == 0:
        return Output(None, metadata={"rows": 0})

    raw_clean_df = enforce_prd_schema(raw_clean_df, MONGOLIA_RAW_CLEAN_SCHEMA)
    raw_clean_filepath = (
        f"gold/qos-raw/MNG/qos_mongolia_{query_date.isoformat()}.parquet"
    )
    ADLSFileClient.upload_raw(None, to_parquet_bytes(raw_clean_df), raw_clean_filepath)
    context.log.info(f"{len(raw_clean_df)} raw_clean rows -> {raw_clean_filepath}")

    gold_df = aggregate_gold_dataframe(raw_clean_df, s, context)
    gold_df = enforce_prd_schema(gold_df, MONGOLIA_GOLD_SCHEMA)
    gold_filepath = f"gold/qos/MNG/qos_mongolia_{query_date.isoformat()}.parquet"
    ADLSFileClient.upload_raw(None, to_parquet_bytes(gold_df), gold_filepath)

    context.log.info(f"{len(gold_df)} gold rows -> {gold_filepath}")
    return Output(
        None,
        metadata={
            "raw_clean_rows": len(raw_clean_df),
            "gold_rows": len(gold_df),
            "filepath": gold_filepath,
        },
    )
