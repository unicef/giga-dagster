"""VCT Meraki daily combined events + QoS — helpers and data-fetch logic."""

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
from src.custom.qos.vct.events_daily import (
    EVENT_COLUMNS,
    _day_window_iso,
    _format_five_min_window,
    _normalize_changes,
    _parse_meraki_name_room,
)
from src.settings import settings

from dagster import OpExecutionContext

DAY_RESOLUTION_SECONDS = 3600
DATA_RATE_SLEEP_SECONDS = 0.05
LATENCY_HISTORY_SLEEP_SECONDS = 0.05

QOS_DAY_COLUMNS = [
    "downstream_total_packet",
    "downstream_packet_lost",
    "loss_pct",
    "avg_latency_ms",
    "download_kbps",
    "upload_kbps",
    "avg_kbps",
    "downtime_s",
    "uptime_pct",
    "window_start",
    "window_end",
    "collected_at",
]

COMBINED_COLUMNS = EVENT_COLUMNS + QOS_DAY_COLUMNS


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


def _compute_uptime(changes_df: pd.DataFrame, t0: str, t1: str) -> pd.DataFrame:
    cols = ["serial", "downtime_s", "uptime_pct"]
    if changes_df.empty:
        return pd.DataFrame(columns=cols)
    w0 = pd.to_datetime(t0, utc=True)
    w1 = pd.to_datetime(t1, utc=True)
    window_sec = (w1 - w0).total_seconds()
    results: list[dict[str, Any]] = []
    for serial, grp in changes_df.groupby("serial", sort=False):
        cur_status: Optional[str] = None
        cur_time = w0
        down_sec = 0.0
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
            cur_time = t
            cur_status = row["newStatus"]
        if cur_status is None:
            continue
        seg = (w1 - cur_time).total_seconds()
        if cur_status == "offline":
            down_sec += max(0.0, seg)
        results.append(
            {
                "serial": serial,
                "downtime_s": round(down_sec, 3),
                "uptime_pct": round(100.0 * (1.0 - down_sec / window_sec), 3),
            }
        )
    return pd.DataFrame(results)


def _mean(buckets: Optional[list], key: str) -> Optional[float]:
    if not buckets:
        return None
    vals = [b[key] for b in buckets if b.get(key) is not None]
    return sum(float(v) for v in vals) / len(vals) if vals else None


def _fetch_packet_loss(db: meraki.DashboardAPI, t0: str, t1: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for org_id, network_ids in NETWORK_DICT.items():
        raw = call(
            db.wireless.getOrganizationWirelessDevicesPacketLossByDevice,
            org_id,
            networkIds=network_ids,
            t0=t0,
            t1=t1,
            total_pages="all",
        )
        rows = []
        for r in raw or []:
            downstream = r.get("downstream") or {}
            rows.append(
                {
                    "serial": (r.get("device") or {}).get("serial"),
                    "downstream_total_packet": downstream.get("total"),
                    "downstream_packet_lost": downstream.get("lost"),
                    "loss_pct": downstream.get("lossPercentage"),
                }
            )
        frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame(
            columns=[
                "serial",
                "downstream_total_packet",
                "downstream_packet_lost",
                "loss_pct",
            ]
        )
    return pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["serial"], keep="first"
    )


def _fetch_latency_df(
    db: meraki.DashboardAPI,
    inventory: pd.DataFrame,
    t0: str,
    t1: str,
    context: OpExecutionContext,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, ap in inventory.iterrows():
        serial = str(ap["serial"])
        try:
            buckets = call(
                db.wireless.getNetworkWirelessLatencyHistory,
                str(ap["networkId"]),
                t0=t0,
                t1=t1,
                resolution=DAY_RESOLUTION_SECONDS,
                deviceSerial=serial,
            )
            avg_ms = _mean(buckets, "avgLatencyMs")
        except meraki.APIError as exc:
            context.log.warning(f"Latency fetch failed for {serial}: {exc.status}")
            avg_ms = None
        rows.append({"serial": serial, "avg_latency_ms": avg_ms})
        if LATENCY_HISTORY_SLEEP_SECONDS:
            time.sleep(LATENCY_HISTORY_SLEEP_SECONDS)
    return pd.DataFrame(rows)


def _fetch_data_rate_df(
    db: meraki.DashboardAPI,
    inventory: pd.DataFrame,
    t0: str,
    t1: str,
    context: OpExecutionContext,
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
                t0=t0,
                t1=t1,
                resolution=DAY_RESOLUTION_SECONDS,
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
                t0=t0,
                t1=t1,
                resolution=DAY_RESOLUTION_SECONDS,
                deviceSerial=serial,
            )
            rows.append(
                {
                    "serial": serial,
                    "download_kbps": _mean(buckets, "downloadKbps"),
                    "upload_kbps": _mean(buckets, "uploadKbps"),
                    "avg_kbps": _mean(buckets, "averageKbps"),
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


def _fetch_day_changes(
    db: meraki.DashboardAPI, day_t0: str, day_t1: str
) -> pd.DataFrame:
    """Fetch and normalize change history for all orgs for one UTC day."""
    day_start = pd.to_datetime(day_t0, utc=True)
    day_end = pd.to_datetime(day_t1, utc=True)
    frames: list[pd.DataFrame] = []
    for org_id, network_ids in NETWORK_DICT.items():
        org_label = ORG_NAMES.get(org_id, org_id)
        raw = call(
            db.organizations.getOrganizationDevicesAvailabilitiesChangeHistory,
            org_id,
            networkIds=network_ids,
            productTypes=["wireless"],
            t0=day_t0,
            t1=day_t1,
            total_pages="all",
        )
        df = _normalize_changes(raw)
        if not df.empty:
            mask = (df["occurredAt"] >= day_start) & (df["occurredAt"] < day_end)
            df = df.loc[mask].copy()
            if not df.empty:
                df["organization"] = org_label
                df["meraki_name_room"] = df["deviceName"].map(_parse_meraki_name_room)
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _build_events_df(
    changes_df: pd.DataFrame,
    report_day: dt.date,
    lookup: pd.DataFrame,
) -> pd.DataFrame:
    events_df = changes_df.copy()
    events_df["report_date"] = report_day.isoformat()
    if not events_df.empty and "occurredAt" in events_df.columns:
        ts = pd.to_datetime(events_df["occurredAt"], utc=True)
        events_df["five_min_window"] = ts.map(_format_five_min_window)
        events_df["changes_count_day"] = events_df.groupby("serial", sort=False)[
            "serial"
        ].transform("count")
    if not lookup.empty:
        events_df = events_df.merge(lookup, on="serial", how="left")
    else:
        events_df["school_id_govt"] = None
        events_df["school_id_giga"] = None
    for col in EVENT_COLUMNS:
        if col not in events_df.columns:
            events_df[col] = None
    return events_df[EVENT_COLUMNS]


def build_combined_dataframe(
    report_day: dt.date,
    spark_session: SparkSession,
    context: OpExecutionContext,
) -> pd.DataFrame:
    db = create_dashboard(settings.VCT_MERAKI_API_KEY)
    day_t0, day_t1 = _day_window_iso(report_day)

    changes_df = _fetch_day_changes(db, day_t0, day_t1)
    lookup = _load_school_lookup(spark_session, context)
    events_df = _build_events_df(changes_df, report_day, lookup)

    if events_df.empty:
        return pd.DataFrame(columns=COMBINED_COLUMNS)

    inventory = _build_ap_inventory(db, context)
    uptime_df = _compute_uptime(
        changes_df[["serial", "occurredAt", "previousStatus", "newStatus"]].copy()
        if not changes_df.empty
        else pd.DataFrame(),
        day_t0,
        day_t1,
    )
    packet_loss_df = _fetch_packet_loss(db, day_t0, day_t1)
    context.log.info("Fetching latency history per AP...")
    latency_df = _fetch_latency_df(db, inventory, day_t0, day_t1, context)
    context.log.info("Fetching data rate per AP...")
    data_rate_df = _fetch_data_rate_df(db, inventory, day_t0, day_t1, context)

    collected_at = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
    qos_df = inventory.merge(uptime_df, on="serial", how="left")
    qos_df = qos_df.merge(packet_loss_df, on="serial", how="left")
    qos_df = qos_df.merge(latency_df, on="serial", how="left")
    qos_df = qos_df.merge(data_rate_df, on="serial", how="left")
    qos_df["window_start"] = day_t0
    qos_df["window_end"] = day_t1
    qos_df["collected_at"] = collected_at

    qos_cols = ["serial"] + QOS_DAY_COLUMNS
    qos_subset = qos_df[[c for c in qos_cols if c in qos_df.columns]]

    combined = events_df.merge(qos_subset, on="serial", how="left", validate="m:1")
    for col in COMBINED_COLUMNS:
        if col not in combined.columns:
            combined[col] = None

    return combined[COMBINED_COLUMNS]
