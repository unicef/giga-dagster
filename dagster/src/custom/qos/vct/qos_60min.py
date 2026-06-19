"""VCT Meraki 60-minute QoS snapshot — helpers and data-fetch logic."""

from __future__ import annotations

import datetime as dt
import time
from typing import Any, Optional

import meraki
import pandas as pd
from pyspark.sql import (
    SparkSession,
    functions as F,
)
from src.custom.qos.meraki_client import call, create_dashboard
from src.custom.qos.vct.constants import NETWORK_DICT, ORG_NAMES
from src.settings import settings

from dagster import OpExecutionContext

TIMESPAN_SECONDS = 3600
DATA_RATE_TIMESPAN_SECONDS = 3600
DATA_RATE_RESOLUTION_SECONDS = 3600
DATA_RATE_SLEEP_SECONDS = 0.05
LATENCY_HISTORY_TIMESPAN_SECONDS = 3600
LATENCY_HISTORY_RESOLUTION_SECONDS = 3600
LATENCY_HISTORY_SLEEP_SECONDS = 0.05

SNAPSHOT_COLUMNS = [
    "serial",
    "deviceName",
    "model",
    "productType",
    "networkId",
    "networkName",
    "organization",
    "meraki_name_room",
    "downstream_total_packet",
    "downstream_packet_lost",
    "loss_pct",
    "avg_latency_ms",
    "download_kbps",
    "upload_kbps",
    "avg_kbps",
    "downtime_s",
    "uptime_pct",
    "time_window",
    "window_start",
    "window_end",
    "timespan_s",
    "collected_at",
    "school_id_govt",
    "school_id_giga",
]


def _parse_meraki_name_room(device_name: Optional[str]) -> str:
    if not isinstance(device_name, str) or not device_name:
        return ""
    parts = device_name.split(" - ", 1)
    return parts[1].strip() if len(parts) > 1 else device_name.strip()


def _pick_detail(details: Optional[dict], which: str, key: str) -> Optional[str]:
    for item in (details or {}).get(which) or []:
        if item.get("name") == key:
            return item.get("value")
    return None


def _align_window(timespan_s: int) -> tuple[dt.datetime, dt.datetime, str, str]:
    now = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    bucket_min = timespan_s // 60
    aligned_min = (now.minute // bucket_min) * bucket_min
    end = now.replace(minute=aligned_min)
    start = end - dt.timedelta(seconds=timespan_s)
    t0 = start.isoformat().replace("+00:00", "Z")
    t1 = end.isoformat().replace("+00:00", "Z")
    return start, end, t0, t1


def _format_time_window(start: dt.datetime, bucket_minutes: int) -> str:
    end = start + dt.timedelta(minutes=bucket_minutes)
    return f"{start.hour:02d}.{start.minute:02d} - {end.hour:02d}.{end.minute:02d}"


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
                    context.log.warning(f"Network {net_id} not found, skipping")
                    continue
                raise
            try:
                devs = call(db.networks.getNetworkDevices, net_id)
            except meraki.APIError as exc:
                if exc.status == 404:
                    context.log.warning(f"Devices for {net_id} not found, skipping")
                    continue
                raise
            for d in devs:
                if not (d.get("model") or "").upper().startswith("MR"):
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
    if df.empty:
        return df
    return df.drop_duplicates(subset=["serial"], keep="first").reset_index(drop=True)


def _normalize_changes(raw_rows: Optional[list]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for r in raw_rows or []:
        det = r.get("details") or {}
        occurred_at = r.get("ts") or r.get("time") or r.get("occurredAt")
        rows.append(
            {
                "serial": (r.get("device") or {}).get("serial"),
                "occurredAt": occurred_at,
                "previousStatus": _pick_detail(det, "old", "status"),
                "newStatus": _pick_detail(det, "new", "status"),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["occurredAt"] = pd.to_datetime(df["occurredAt"], utc=True, errors="coerce")
    df = df.dropna(subset=["serial", "occurredAt"])
    df = df[df["previousStatus"] != df["newStatus"]].copy()
    return df.sort_values(["serial", "occurredAt"]).reset_index(drop=True)


def _walk_serial_intervals(
    grp: pd.DataFrame,
    w0: pd.Timestamp,
    w1: pd.Timestamp,
    count_alerting: bool,
) -> Optional[tuple[float, float]]:
    """Return (downtime_s, uptime_pct) for one AP, or None if no usable history."""
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
    denom = (down_sec + alert_sec) if count_alerting else down_sec
    window_sec = (w1 - w0).total_seconds()
    return round(denom, 3), round(100.0 * (1.0 - denom / window_sec), 3)


def _compute_uptime(
    changes_df: pd.DataFrame, t0: str, t1: str, count_alerting: bool = False
) -> pd.DataFrame:
    cols = ["serial", "downtime_s", "uptime_pct"]
    if changes_df.empty:
        return pd.DataFrame(columns=cols)
    w0 = pd.to_datetime(t0, utc=True)
    w1 = pd.to_datetime(t1, utc=True)
    results: list[dict[str, Any]] = []
    for serial, grp in changes_df.groupby("serial", sort=False):
        result = _walk_serial_intervals(grp, w0, w1, count_alerting)
        if result is None:
            continue
        down_s, uptime_pct = result
        results.append(
            {
                "serial": serial,
                "downtime_s": down_s,
                "uptime_pct": uptime_pct,
            }
        )
    return pd.DataFrame(results)


def _normalize_packet_loss(raw_rows: Optional[list]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for r in raw_rows or []:
        downstream = r.get("downstream") or {}
        rows.append(
            {
                "serial": (r.get("device") or {}).get("serial"),
                "downstream_total_packet": downstream.get("total"),
                "downstream_packet_lost": downstream.get("lost"),
                "loss_pct": downstream.get("lossPercentage"),
            }
        )
    cols = ["serial", "downstream_total_packet", "downstream_packet_lost", "loss_pct"]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows).drop_duplicates(subset=["serial"], keep="first")


def _fetch_latency_df(
    db: meraki.DashboardAPI, inventory: pd.DataFrame, context: OpExecutionContext
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, ap in inventory.iterrows():
        serial = str(ap["serial"])
        try:
            buckets = call(
                db.wireless.getNetworkWirelessLatencyHistory,
                str(ap["networkId"]),
                timespan=LATENCY_HISTORY_TIMESPAN_SECONDS,
                resolution=LATENCY_HISTORY_RESOLUTION_SECONDS,
                deviceSerial=serial,
            )
            avg_ms = buckets[0].get("avgLatencyMs") if buckets else None
        except meraki.APIError as exc:
            context.log.warning(f"Latency fetch failed for {serial}: {exc.status}")
            avg_ms = None
        rows.append({"serial": serial, "avg_latency_ms": avg_ms})
        if LATENCY_HISTORY_SLEEP_SECONDS:
            time.sleep(LATENCY_HISTORY_SLEEP_SECONDS)
    return pd.DataFrame(rows)


def _fetch_data_rate_df(
    db: meraki.DashboardAPI, inventory: pd.DataFrame, context: OpExecutionContext
) -> pd.DataFrame:
    cols = ["serial", "download_kbps", "upload_kbps", "avg_kbps"]
    if inventory.empty:
        return pd.DataFrame(columns=cols)

    unsupported: set[str] = set()
    for net_id, grp in inventory.groupby("networkId", sort=False):
        sample = str(grp.iloc[0]["serial"])
        try:
            call(
                db.wireless.getNetworkWirelessDataRateHistory,
                str(net_id),
                timespan=DATA_RATE_TIMESPAN_SECONDS,
                resolution=DATA_RATE_RESOLUTION_SECONDS,
                deviceSerial=sample,
            )
        except meraki.APIError as exc:
            if exc.status == 400 and "MR 27.0" in str(
                getattr(exc, "message", "") or exc
            ):
                unsupported.add(str(net_id))
                context.log.warning(
                    f"Data rate unavailable for network {net_id} (requires MR 27.0+)"
                )
            else:
                raise
        if DATA_RATE_SLEEP_SECONDS:
            time.sleep(DATA_RATE_SLEEP_SECONDS)

    rows: list[dict[str, Any]] = []
    for _, ap in inventory.iterrows():
        net_id = str(ap["networkId"])
        serial = str(ap["serial"])
        if net_id in unsupported:
            rows.append(
                {
                    "serial": serial,
                    "download_kbps": None,
                    "upload_kbps": None,
                    "avg_kbps": None,
                }
            )
            continue
        try:
            buckets = call(
                db.wireless.getNetworkWirelessDataRateHistory,
                net_id,
                timespan=DATA_RATE_TIMESPAN_SECONDS,
                resolution=DATA_RATE_RESOLUTION_SECONDS,
                deviceSerial=serial,
            )
            row = buckets[0] if buckets else {}
            rows.append(
                {
                    "serial": serial,
                    "download_kbps": row.get("downloadKbps"),
                    "upload_kbps": row.get("uploadKbps"),
                    "avg_kbps": row.get("averageKbps"),
                }
            )
        except meraki.APIError as exc:
            context.log.warning(f"Data rate fetch failed for {serial}: {exc.status}")
            rows.append(
                {
                    "serial": serial,
                    "download_kbps": None,
                    "upload_kbps": None,
                    "avg_kbps": None,
                }
            )
        if DATA_RATE_SLEEP_SECONDS:
            time.sleep(DATA_RATE_SLEEP_SECONDS)

    return pd.DataFrame(rows)


def _load_school_lookup(
    spark_session: SparkSession, context: OpExecutionContext
) -> pd.DataFrame:
    try:
        device_meta = (
            spark_session.read.table("custom_dataset.device_matched")
            .select(
                F.col("serial"),
                F.col("school_id_govt").cast("string").alias("school_id_govt"),
            )
            .toPandas()
        )
        school_master = (
            spark_session.read.table("school_master.vct")
            .select(
                F.col("school_id_govt").cast("string").alias("school_id_govt"),
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


def build_snapshot_dataframe(
    spark_session: SparkSession,
    context: OpExecutionContext,
    window_start: dt.datetime,
    window_end: dt.datetime,
) -> pd.DataFrame:
    t0 = window_start.isoformat().replace("+00:00", "Z")
    t1 = window_end.isoformat().replace("+00:00", "Z")
    db = create_dashboard(settings.VCT_MERAKI_API_KEY)
    context.log.info(f"Window: {t0} → {t1}")

    inventory = _build_ap_inventory(db, context)
    context.log.info(f"Inventory: {len(inventory)} APs")
    if inventory.empty:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    changes_frames: list[pd.DataFrame] = []
    for org_id, network_ids in NETWORK_DICT.items():
        raw = call(
            db.organizations.getOrganizationDevicesAvailabilitiesChangeHistory,
            org_id,
            networkIds=network_ids,
            productTypes=["wireless"],
            t0=t0,
            t1=t1,
            total_pages="all",
        )
        changes_frames.append(_normalize_changes(raw))
    changes_df = (
        pd.concat(changes_frames, ignore_index=True)
        if changes_frames
        else pd.DataFrame()
    )

    packet_loss_frames: list[pd.DataFrame] = []
    for org_id, network_ids in NETWORK_DICT.items():
        raw = call(
            db.wireless.getOrganizationWirelessDevicesPacketLossByDevice,
            org_id,
            networkIds=network_ids,
            t0=t0,
            t1=t1,
            total_pages="all",
        )
        packet_loss_frames.append(_normalize_packet_loss(raw))
    packet_loss_df = (
        pd.concat(packet_loss_frames, ignore_index=True)
        if packet_loss_frames
        else pd.DataFrame(
            columns=[
                "serial",
                "downstream_total_packet",
                "downstream_packet_lost",
                "loss_pct",
            ]
        )
    )

    context.log.info("Fetching latency history per AP...")
    latency_df = _fetch_latency_df(db, inventory, context)

    context.log.info("Fetching data rate per AP...")
    data_rate_df = _fetch_data_rate_df(db, inventory, context)

    uptime_df = _compute_uptime(changes_df, t0, t1)
    collected_at = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")

    out = inventory.merge(uptime_df, on="serial", how="left")
    out = out.merge(packet_loss_df, on="serial", how="left")
    out = out.merge(latency_df, on="serial", how="left")
    out = out.merge(data_rate_df, on="serial", how="left")

    bucket_minutes = max(1, TIMESPAN_SECONDS // 60)
    out["time_window"] = _format_time_window(window_start, bucket_minutes)
    out["window_start"] = t0
    out["window_end"] = t1
    out["timespan_s"] = TIMESPAN_SECONDS
    out["collected_at"] = collected_at

    lookup = _load_school_lookup(spark_session, context)
    if not lookup.empty:
        out = out.merge(lookup, on="serial", how="left")
    else:
        out["school_id_govt"] = None
        out["school_id_giga"] = None

    for col in SNAPSHOT_COLUMNS:
        if col not in out.columns:
            out[col] = None

    return out[SNAPSHOT_COLUMNS]
