"""Mongolia device QoS fetch and transform logic — ports get_bandwidth_utilization.py,
process_mongolia_raw.py, and process_mongolia_clean.py's prod path (not
process_bandwidth_utilization.py's independent staging-only pipeline, which is not
ported - see schema.py)."""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import requests
from pyspark.sql import SparkSession, functions as F
from src.custom.qos.mongolia.constants import CONVERSION_FACTORS, REFERENCE_DEVICES_FILEPATH
from src.settings import settings
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext


def fetch_device_snapshots(
    adls_file_client: ADLSFileClient, run_timestamp, context: OpExecutionContext
) -> None:
    """Polls every whitelisted device and uploads its raw JSON response to ADLS.
    Ports get_bandwidth_utilization.py - runs every 5 minutes."""
    devices_df = adls_file_client.download_csv_as_pandas_dataframe(REFERENCE_DEVICES_FILEPATH)
    headers = {"Authorization": f"Bearer {settings.MONGOLIA_DEVICE_BEARER_TOKEN}"}
    stamp = run_timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    date_prefix = run_timestamp.date().isoformat()

    for device_id, url in zip(devices_df["device_id"], devices_df["fetch_url"]):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            payload = json.loads(response.text)
            port = payload["port"]
            port["source_device_id"] = device_id
            port["api_fetch_timestamp"] = stamp

            filepath = f"raw/qos/MNG/{date_prefix}/qos_mongolia_{device_id}_{stamp}.json"
            ADLSFileClient.upload_raw(None, response.content, filepath)
        except Exception as exc:
            context.log.error(f"Error getting data for device {device_id}: {exc}")


def _load_school_lookup(
    spark_session: SparkSession, context: OpExecutionContext
) -> pd.DataFrame:
    try:
        return (
            spark_session.read.table("school_master.mng")
            .select(
                F.col("school_id_govt").cast("string").alias("school_id_govt"),
                F.col("school_id_giga").cast("string").alias("school_id_giga"),
            )
            .toPandas()
        )
    except Exception as exc:
        context.log.warning(
            f"school_master.mng lookup unavailable, school_id_giga will be null: {exc}"
        )
        return pd.DataFrame(columns=["school_id_govt", "school_id_giga"])


def build_raw_clean_dataframe(
    query_date: date,
    adls_file_client: ADLSFileClient,
    spark_session: SparkSession,
    context: OpExecutionContext,
) -> pd.DataFrame:
    """Reads a day's device JSON snapshots from ADLS and cleans them into the
    raw_clean shape. Ports process_mongolia_raw.py's clean_output()."""
    prefix = f"raw/qos/MNG/{query_date.isoformat()}"
    paths = adls_file_client.list_paths(prefix, recursive=True)

    output = []
    for path in paths:
        name = getattr(path, "name", None)
        if not name or not name.endswith(".json"):
            continue
        try:
            raw = adls_file_client.download_raw(name)
            record = json.loads(raw)["port"]
        except Exception as exc:
            context.log.warning(f"Could not parse {name}: {exc}")
            continue
        record["source_filename"] = name
        output.append(record)

    if not output:
        context.log.warning(f"No raw snapshots found for {query_date}")
        return pd.DataFrame()

    ddf = pd.DataFrame(output)
    ddf = ddf.drop_duplicates(["poll_time", "device_id"])

    ddf = ddf[~ddf["port_id"].isnull()]
    ddf["port_id"] = ddf["port_id"].astype(int)
    ddf = ddf[~ddf["device_id"].isnull()]
    ddf["device_id"] = ddf["device_id"].astype(int)

    devices_df = adls_file_client.download_csv_as_pandas_dataframe(REFERENCE_DEVICES_FILEPATH)
    devices_df = devices_df[[
        "device_id", "purpose", "inserted", "hostname", "sysName", "sysContact",
        "version", "hardware", "location", "lat", "lng", "network_tech",
    ]]
    ddf = pd.merge(devices_df, ddf, on="device_id", how="inner")
    ddf["poll_time"] = pd.to_datetime(ddf["poll_time"], unit="s")
    ddf["purpose"] = ddf["purpose"].astype(int)
    ddf["ifVlan"] = ddf["ifVlan"].replace("", float("nan"))
    ddf = ddf.rename(columns={"purpose": "school_id_govt"})

    giga_master = _load_school_lookup(spark_session, context)
    merged_df = pd.merge(ddf, giga_master, on="school_id_govt", how="left")

    for direction in ("in_rate", "out_rate"):
        value_col = f"{direction}_value"
        units_col = f"{direction}_units"
        merged_df[value_col] = merged_df[direction].str.extract(r"(\d+) (\w+)")[0]
        merged_df[units_col] = merged_df[direction].str.extract(r"(\d+) (\w+)")[1]
        merged_df[value_col] = pd.to_numeric(merged_df[value_col])
        merged_df[direction] = merged_df.apply(
            lambda row, d=direction: row[f"{d}_value"] * CONVERSION_FACTORS[row[f"{d}_units"]],
            axis=1,
        )

    merged_df["ifInOctets"] = merged_df["ifInOctets"] / 1048576
    merged_df["ifOutOctets"] = merged_df["ifOutOctets"] / 1048576

    merged_df = merged_df.rename(columns={
        "poll_time": "timestamp",
        "in_rate": "speed_download",
        "out_rate": "speed_upload",
        "ifInOctets": "inbound_traffic",
        "ifOutOctets": "outbound_traffic",
    })
    merged_df["country_id"] = "MNG"
    merged_df["measurement_id"] = "MNG001"

    columns = [
        "timestamp", "school_id_govt", "school_id_giga", "speed_download", "speed_upload",
        "inbound_traffic", "outbound_traffic", "in_perc", "out_perc", "device_id",
        "country_id", "measurement_id",
    ]
    raw_clean_df = merged_df[columns]
    raw_clean_df = raw_clean_df[~raw_clean_df["school_id_giga"].isnull()]
    return raw_clean_df


def aggregate_gold_dataframe(
    raw_clean_df: pd.DataFrame, spark_session: SparkSession, context: OpExecutionContext
) -> pd.DataFrame:
    """Ports process_mongolia_clean.py's hourly aggregation."""
    ddf = raw_clean_df.sort_values("timestamp")
    ddf = ddf.drop_duplicates()

    ddf["timestamp"] = pd.to_datetime(ddf["timestamp"], format="mixed")
    ddf["timestamp"] = ddf["timestamp"].dt.floor("H")
    ddf["count_dummy"] = 1

    ddf = ddf.groupby(["timestamp", "device_id", "school_id_govt"]).agg({
        "speed_download": ["min", "mean", "max"],
        "speed_upload": ["min", "mean", "max"],
        "inbound_traffic": "sum",
        "outbound_traffic": "sum",
        "count_dummy": "sum",
    }).reset_index()

    ddf.columns = [
        "timestamp", "device_id", "school_id_govt",
        "speed_download_min", "speed_download_mean", "speed_download_max",
        "speed_upload_min", "speed_upload_mean", "speed_upload_max",
        "inbound_traffic_sum", "outbound_traffic_sum",
        "count",
    ]

    giga_master = _load_school_lookup(spark_session, context)
    merged_df = pd.merge(ddf, giga_master, on="school_id_govt", how="left")

    merged_df["country_id"] = "MNG"
    merged_df["provider"] = "Mongolia"
    merged_df["measurement_type"] = "usage"

    gold_df = merged_df[[
        "timestamp", "country_id", "school_id_giga", "school_id_govt",
        "provider", "measurement_type", "count",
        "speed_download_min", "speed_download_mean", "speed_download_max",
        "speed_upload_min", "speed_upload_mean", "speed_upload_max",
        "inbound_traffic_sum", "outbound_traffic_sum",
    ]]
    return gold_df
