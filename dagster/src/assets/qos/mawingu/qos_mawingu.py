import datetime as dt

from src.custom.qos.mawingu.common import aggregate_gold_dataframe, fetch_silver_dataframe
from src.custom.qos.mawingu.constants import COUNTRY_CODE
from src.custom.qos.mawingu.schema import MAWINGU_GOLD_SCHEMA
from src.custom.qos.schema_utils import enforce_prd_schema, to_parquet_bytes
from src.utils.adls import ADLSFileClient

from dagster import DailyPartitionsDefinition, OpExecutionContext, Output, asset

# Adjust to Mawingu's actual go-live date before relying on backfills before this.
MAWINGU_QOS_START_DATE = "2024-01-01"


@asset(partitions_def=DailyPartitionsDefinition(start_date=MAWINGU_QOS_START_DATE))
def mawingu_qos(context: OpExecutionContext, adls_file_client: ADLSFileClient) -> Output:
    """Fetches one day's Zabbix history for Mawingu, aggregates hourly, and publishes
    gold/qos/KEN. Combines process_mawingu_raw.py + aggregate_mawingu.py into a single
    in-memory run - the original split existed only to hand a file between two cron
    jobs, which Dagster doesn't need. Only publishes to ADLS; the deployment's own
    container determines dev/stg/prd, unlike the VM script's explicit dual upload.
    The partition key is the completed day being queried - a live run targets
    yesterday, a backfill run targets whichever past day."""
    target_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    silver_df = fetch_silver_dataframe(target_date, adls_file_client, context)
    ADLSFileClient.upload_raw(
        None,
        to_parquet_bytes(silver_df),
        f"silver/qos/{COUNTRY_CODE}/mawingu_{target_date.isoformat()}.parquet",
    )
    context.log.info(f"{len(silver_df)} silver rows")

    gold_df = aggregate_gold_dataframe(silver_df)
    gold_df = enforce_prd_schema(gold_df, MAWINGU_GOLD_SCHEMA)
    gold_parquet = to_parquet_bytes(gold_df)
    gold_filepath = f"gold/qos/{COUNTRY_CODE}/mawingu_{target_date.isoformat()}.parquet"
    ADLSFileClient.upload_raw(None, gold_parquet, gold_filepath)

    context.log.info(f"{len(gold_df)} gold rows -> {gold_filepath}")
    return Output(None, metadata={"rows": len(gold_df), "filepath": gold_filepath})
