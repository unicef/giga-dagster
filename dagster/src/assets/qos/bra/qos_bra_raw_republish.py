import datetime as dt

import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark.sql import (
    SparkSession,
    functions as F,
)
from src.custom.qos.bra.constants import COUNTRY_CODE
from src.custom.qos.schema_utils import to_parquet_bytes
from src.utils.adls import ADLSFileClient

from dagster import DailyPartitionsDefinition, OpExecutionContext, Output, asset

# published by the generic school_qos__gold_csv_to_deltatable_sensor once bra_qos's
# gold/qos/BRA output lands and is ingested - see src/sensors/adhoc.py
GOLD_TABLE_NAME = "qos.bra"

# Adjust to BRA's actual go-live date before relying on backfills before this.
BRA_QOS_RAW_REPUBLISH_START_DATE = "2024-01-01"


@asset(
    partitions_def=DailyPartitionsDefinition(
        start_date=BRA_QOS_RAW_REPUBLISH_START_DATE
    )
)
def bra_qos_raw_republish(
    context: OpExecutionContext, spark: PySparkResource
) -> Output:
    """Republishes one day's gold.qos.BRA rows to gold/qos-raw/BRA/, matching the
    dummy device-level columns a genuine raw table would have but BRA never produces
    (BRA ingests pre-aggregated speed-test data, not per-device polling). Ports
    update_qos_brazil.py. The partition key is the completed day being republished
    - a live run targets yesterday, a backfill run targets whichever past day."""
    s: SparkSession = spark.spark_session
    report_day = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    sdf = s.read.table(GOLD_TABLE_NAME).where(F.col("date") == report_day.isoformat())
    df = sdf.toPandas()

    if len(df) == 0:
        context.log.warning(f"No rows found in {GOLD_TABLE_NAME} for {report_day}")
        return Output(None, metadata={"rows": 0})

    df["measurement_id"] = "BRA001"
    df["inbound_traffic"] = pd.NA
    df["outbound_traffic"] = pd.NA
    df["in_perc"] = pd.NA
    df["out_perc"] = pd.NA
    df["device_id"] = ""

    filepath = f"gold/qos-raw/{COUNTRY_CODE}/{report_day.isoformat()}.parquet"
    ADLSFileClient.upload_raw(None, to_parquet_bytes(df), filepath)

    context.log.info(f"{len(df)} rows republished -> {filepath}")
    return Output(None, metadata={"rows": len(df), "filepath": filepath})
