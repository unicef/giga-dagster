"""Brazil (nic.br) QoS fetch and transform logic — ports get_qos_brazil.py's prod path."""

from __future__ import annotations

import json

import pandas as pd
import requests
from pyspark.sql import SparkSession, functions as F
from src.settings import settings

from dagster import OpExecutionContext

RENAME_COLUMNS = {
    "school_code": "school_id_govt",
    "tcp_down_median_mbps": "speed_download",
    "tcp_up_median_mbps": "speed_upload",
    "rtt_median_ms": "roundtrip_time",
    "jitter_download_ms": "jitter_download",
    "jitter_upload_ms": "jitter_upload",
    "rtt_lost_package_pct": "rtt_packet_loss_pct",
}

GOLD_COLUMNS = [
    "timestamp", "country_id", "school_id_govt", "school_id_giga",
    "speed_download", "speed_upload", "roundtrip_time",
    "jitter_download", "jitter_upload", "rtt_packet_loss_pct",
    "latency", "provider", "ip_family", "report_id", "agent_id",
]


def _load_school_lookup(
    spark_session: SparkSession, context: OpExecutionContext
) -> pd.DataFrame:
    try:
        return (
            spark_session.read.table("school_master.bra")
            .select(
                F.col("school_id_govt").cast("string").alias("school_id_govt"),
                F.col("school_id_giga").cast("string").alias("school_id_giga"),
            )
            .toPandas()
        )
    except Exception as exc:
        context.log.warning(
            f"school_master.bra lookup unavailable, school_id_giga will be null: {exc}"
        )
        return pd.DataFrame(columns=["school_id_govt", "school_id_giga"])


def fetch_and_build_dataframes(
    query_date: str, spark_session: SparkSession, context: OpExecutionContext
) -> tuple[bytes, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Returns (raw_json_bytes, silver_df, gold_df, error_df) for the given day (YYYY-MM-DD)."""
    response = requests.get(
        settings.BRAZIL_API_URL, params={"dayofyear": query_date}, timeout=120
    )
    raw_bytes = response.content
    records = json.loads(response.text)

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["time"])
    df["country_id"] = "BRA"
    df = df.rename(columns=RENAME_COLUMNS)
    df["provider"] = "nic.br"
    df["latency"] = None

    df = df.sort_values(["school_id_govt", "timestamp"])
    df = df[[
        "timestamp", "country_id", "school_id_govt", "speed_download", "speed_upload",
        "roundtrip_time", "jitter_download", "jitter_upload", "rtt_packet_loss_pct",
        "latency", "provider", "ip_family", "report_id", "agent_id",
    ]]

    error_df = df[df["school_id_govt"].isna()].copy()
    df = df[~df["school_id_govt"].isna()].copy()
    df["school_id_govt"] = df["school_id_govt"].astype(int).astype(str)

    giga_master = _load_school_lookup(spark_session, context)
    merged_df = pd.merge(df, giga_master, on="school_id_govt", how="left")
    merged_df = merged_df[GOLD_COLUMNS]

    missing_mask = merged_df["ip_family"].isna() | merged_df["timestamp"].isna()
    missing_df = merged_df[missing_mask].copy()
    missing_df["error"] = "missing_value"
    error_df = pd.concat([error_df, missing_df])
    merged_df = merged_df[~missing_mask]

    silver_df = merged_df[~merged_df["school_id_giga"].isna()]

    no_match_df = merged_df[merged_df["school_id_giga"].isna()].copy()
    no_match_df["error"] = "no_school_match"
    error_df = pd.concat([error_df, no_match_df])

    if silver_df["ip_family"].dtype == int:
        gold_df = silver_df[silver_df["ip_family"] == 4]
    else:
        context.log.warning(
            "ip_family column is not integer-typed; skipping IPv4 filter for gold"
        )
        gold_df = silver_df

    return raw_bytes, silver_df, gold_df, error_df
