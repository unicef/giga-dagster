"""Shared VCT Meraki helpers used across qos_60min, events_daily, and combined_daily."""

from __future__ import annotations

from typing import Any, Optional

import meraki
import pandas as pd
from pyspark.sql import (
    SparkSession,
    functions as F,
)
from src.custom.qos.meraki_client import call
from src.custom.qos.vct.constants import NETWORK_DICT, ORG_NAMES

from dagster import OpExecutionContext


def _kbps_to_mbps(value: Optional[float]) -> Optional[float]:
    return value / 1000.0 if value is not None else None


def _parse_meraki_name_room(device_name: Optional[str]) -> str:
    if not isinstance(device_name, str) or device_name == "":
        return ""
    parts = device_name.split(" - ", 1)
    if len(parts) > 1:
        return parts[1].strip()
    return device_name.strip()


def _pick_detail(details: Optional[dict], which: str, key: str) -> Optional[str]:
    if details is None:
        details = {}
    for item in details.get(which) or []:
        if item.get("name") == key:
            return item.get("value")
    return None


def _walk_serial_uptime(
    grp: pd.DataFrame,
    w0: pd.Timestamp,
    w1: pd.Timestamp,
    count_alerting_as_downtime: bool,
    window_sec: float,
) -> Optional[tuple[float, float]]:
    cur_status: Optional[str] = None
    cur_time = w0
    down_sec = 0.0
    alert_sec = 0.0
    for _, row in grp.iterrows():
        t = row["occurredAt"]
        if t <= w0:
            cur_status = row["newStatus"]
            continue
        if cur_status is None:
            cur_status = row["previousStatus"]
        if t >= w1:
            break
        seg = (t - cur_time).total_seconds()
        if cur_status == "offline":
            down_sec += max(0.0, seg)
        elif cur_status == "alerting":
            alert_sec += max(0.0, seg)
        cur_time = t
        cur_status = row["newStatus"]
    if cur_status is None:
        return None
    seg = (w1 - cur_time).total_seconds()
    if cur_status == "offline":
        down_sec += max(0.0, seg)
    elif cur_status == "alerting":
        alert_sec += max(0.0, seg)
    denom = (down_sec + alert_sec) if count_alerting_as_downtime else down_sec
    return round(denom, 3), round(100.0 * (1.0 - denom / window_sec), 3)


def _compute_uptime_metrics(
    changes_df: pd.DataFrame,
    t0: str,
    t1: str,
    count_alerting_as_downtime: bool = False,
) -> pd.DataFrame:
    cols = ["serial", "downtime_s", "uptime_percentage"]
    if len(changes_df) == 0:
        return pd.DataFrame(columns=cols)
    w0 = pd.to_datetime(t0, utc=True)
    w1 = pd.to_datetime(t1, utc=True)
    window_sec = (w1 - w0).total_seconds()
    results: list[dict[str, Any]] = []
    for serial, grp in changes_df.groupby("serial", sort=False):
        result = _walk_serial_uptime(
            grp, w0, w1, count_alerting_as_downtime, window_sec
        )
        if result is None:
            continue
        down_s, uptime_percentage = result
        results.append(
            {
                "serial": serial,
                "downtime_s": down_s,
                "uptime_percentage": uptime_percentage,
            }
        )
    return pd.DataFrame(results)


def _build_ap_inventory(
    db: meraki.DashboardAPI, context: OpExecutionContext
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for org_id, net_ids in NETWORK_DICT.items():
        org_label = ORG_NAMES.get(org_id, org_id)
        for net_id in net_ids:
            try:
                net_meta = call(db.networks.getNetwork, net_id)
                net_name = net_meta.get("name", net_id)
            except meraki.APIError as exc:
                if exc.status == 404:
                    context.log.warning(f"Skip network {net_id}: 404")
                    continue
                raise
            try:
                devs = call(db.networks.getNetworkDevices, net_id)
            except meraki.APIError as exc:
                if exc.status == 404:
                    context.log.warning(f"Skip network {net_id}: devices 404")
                    continue
                raise
            for d in devs:
                model = (d.get("model") or "").upper()
                if not model.startswith("MR"):
                    continue
                device_name = d.get("name") or ""
                rows.append(
                    {
                        "serial": d.get("serial"),
                        "deviceName": device_name,
                        "model": d.get("model"),
                        "productType": "wireless",
                        "networkId": net_id,
                        "networkName": net_name,
                        "organization": org_label,
                        "meraki_name_room": _parse_meraki_name_room(device_name),
                    }
                )
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    return df.drop_duplicates(subset=["serial"], keep="first").reset_index(drop=True)


def _normalize_packet_loss(raw_rows: Optional[list]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for r in raw_rows or []:
        dev = r.get("device") or {}
        downstream = r.get("downstream") or {}
        rows.append(
            {
                "serial": dev.get("serial"),
                "downstream_total_packet": downstream.get("total"),
                "downstream_packet_lost": downstream.get("lost"),
                "downstream_loss_pct": downstream.get("lossPercentage"),
            }
        )
    cols = [
        "serial",
        "downstream_total_packet",
        "downstream_packet_lost",
        "downstream_loss_pct",
    ]
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return pd.DataFrame(columns=cols)
    return df.drop_duplicates(subset=["serial"], keep="first")


def _data_rate_network_unsupported(exc: meraki.APIError) -> bool:
    if exc.status != 400:
        return False
    msg = str(getattr(exc, "message", "") or exc)
    return "MR 27.0" in msg


def _load_school_lookup(
    spark_session: SparkSession, context: OpExecutionContext
) -> pd.DataFrame:
    try:
        device_meta = (
            spark_session.read.table("custom_dataset.device_matched")
            .select(
                F.col("serial"),
                F.col("school_id_govt")
                .cast("bigint")
                .cast("string")
                .alias("school_id_govt"),
            )
            .toPandas()
        )
        school_master = (
            spark_session.read.table("school_master.vct")
            .select(
                F.col("school_id_govt")
                .cast("bigint")
                .cast("string")
                .alias("school_id_govt"),
                F.col("school_id_giga").cast("string").alias("school_id_giga"),
            )
            .toPandas()
        )
        lookup = device_meta.merge(school_master, on="school_id_govt", how="left")
        return lookup[["serial", "school_id_govt", "school_id_giga"]].drop_duplicates(
            subset=["serial"], keep="first"
        )
    except Exception as exc:
        context.log.warning(
            f"School lookup unavailable, school IDs will be null: {exc}"
        )
        return pd.DataFrame(columns=["serial", "school_id_govt", "school_id_giga"])


def _attach_school_ids(
    df: pd.DataFrame, spark_session: SparkSession, context: OpExecutionContext
) -> pd.DataFrame:
    lookup = _load_school_lookup(spark_session, context)
    if len(lookup) == 0:
        out = df.copy()
        out["school_id_govt"] = None
        out["school_id_giga"] = None
        return out
    result = df.merge(lookup, on="serial", how="left", validate="m:1")
    result["school_id_govt"] = (
        result["school_id_govt"].astype(str).where(result["school_id_govt"].notna())
    )
    return result
