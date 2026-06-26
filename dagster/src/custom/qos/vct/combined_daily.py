"""VCT Meraki daily combined events + QoS — helpers and data-fetch logic."""

from __future__ import annotations

import datetime as dt
import time
from typing import Any, Optional

import meraki
import pandas as pd
from pyspark.sql import SparkSession
from src.custom.qos.meraki_client import call, create_dashboard
from src.custom.qos.vct.common import (
    _attach_school_ids,
    _build_ap_inventory,
    _compute_uptime_metrics,
    _data_rate_network_unsupported,
    _kbps_to_mbps,
    _normalize_packet_loss,
    _parse_meraki_name_room,
)
from src.custom.qos.vct.constants import NETWORK_DICT, ORG_NAMES
from src.custom.qos.vct.events_daily import (
    EVENT_COLUMNS,
    EVENT_RENAME,
    _add_five_min_window_from_timestamp,
    _day_window_iso,
    _normalize_changes as _normalize_event_changes,
)
from src.settings import settings

from dagster import OpExecutionContext

DAY_RESOLUTION_SECONDS = 3600
DATA_RATE_SLEEP_SECONDS = 0.05
LATENCY_HISTORY_SLEEP_SECONDS = 0.05


QOS_DAY_COLUMNS = [
    "downstream_total_packet",
    "downstream_packet_lost",
    "downstream_loss_pct",
    "avg_latency_ms",
    "download_mbps",
    "upload_mbps",
    "avg_mbps",
    "downtime_s",
    "uptime_percentage",
    "window_end",
    "collected_at",
]

COMBINED_COLUMNS = EVENT_COLUMNS + QOS_DAY_COLUMNS


def _mean_numeric(buckets: Optional[list], key: str) -> Any:
    if not buckets:
        return None
    vals = [b[key] for b in buckets if b.get(key) is not None]
    if not vals:
        return None
    return sum(float(v) for v in vals) / len(vals)


def _fetch_change_history_for_org(
    db: meraki.DashboardAPI, org_id: str, network_ids: list[str], t0: str, t1: str
) -> list:
    return call(
        db.organizations.getOrganizationDevicesAvailabilitiesChangeHistory,
        org_id,
        networkIds=network_ids,
        productTypes=["wireless"],
        t0=t0,
        t1=t1,
        total_pages="all",
    )


def _fetch_day_change_history(
    db: meraki.DashboardAPI, day_t0: str, day_t1: str
) -> pd.DataFrame:
    day_start = pd.to_datetime(day_t0, utc=True)
    day_end = pd.to_datetime(day_t1, utc=True)
    frames: list[pd.DataFrame] = []
    for org_id, network_ids in NETWORK_DICT.items():
        org_label = ORG_NAMES.get(org_id, org_id)
        raw = _fetch_change_history_for_org(db, org_id, network_ids, day_t0, day_t1)
        df = _normalize_event_changes(raw)
        if len(df) > 0:
            day_mask = (df["occurredAt"] >= day_start) & (df["occurredAt"] < day_end)
            df = df.loc[day_mask].copy()
            if len(df) > 0:
                df["organization"] = org_label
                df["meraki_name_room"] = df["deviceName"].map(_parse_meraki_name_room)
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _build_events_from_changes(
    changes_df: pd.DataFrame,
    report_day: dt.date,
    spark_session: SparkSession,
    context: OpExecutionContext,
) -> pd.DataFrame:
    events_df = changes_df.copy()
    events_df["report_date"] = report_day.isoformat()
    if len(events_df) > 0 and "occurredAt" in events_df.columns:
        events_df["five_min_window"] = _add_five_min_window_from_timestamp(
            events_df["occurredAt"]
        )
        events_df["changes_count_day"] = events_df.groupby("serial", sort=False)[
            "serial"
        ].transform("count")
    events_df = _attach_school_ids(events_df, spark_session, context)
    events_df = events_df.rename(columns=EVENT_RENAME)
    for col in EVENT_COLUMNS:
        if col not in events_df.columns:
            events_df[col] = None
    return events_df[EVENT_COLUMNS]


def _probe_data_rate_network(
    db: meraki.DashboardAPI, network_id: str, sample_serial: str, t0: str, t1: str
) -> bool:
    try:
        call(
            db.wireless.getNetworkWirelessDataRateHistory,
            network_id,
            t0=t0,
            t1=t1,
            resolution=DAY_RESOLUTION_SECONDS,
            deviceSerial=sample_serial,
        )
        return True
    except meraki.APIError as exc:
        if _data_rate_network_unsupported(exc):
            return False
        raise


def _fetch_ap_data_rate_row(
    db: meraki.DashboardAPI,
    ap: pd.Series,
    unsupported_networks: set[str],
    cols: list[str],
    t0: str,
    t1: str,
    context: OpExecutionContext,
) -> tuple[dict[str, Any], int, int]:
    network_id = str(ap["networkId"])
    serial = str(ap["serial"])
    if network_id in unsupported_networks:
        row: dict[str, Any] = {c: None for c in cols}
        row["serial"] = serial
        return row, 0, 1
    try:
        buckets = call(
            db.wireless.getNetworkWirelessDataRateHistory,
            network_id,
            t0=t0,
            t1=t1,
            resolution=DAY_RESOLUTION_SECONDS,
            deviceSerial=serial,
        )
        row = {
            "serial": serial,
            "download_mbps": _kbps_to_mbps(_mean_numeric(buckets, "downloadKbps")),
            "upload_mbps": _kbps_to_mbps(_mean_numeric(buckets, "uploadKbps")),
            "avg_mbps": _kbps_to_mbps(_mean_numeric(buckets, "averageKbps")),
        }
        if DATA_RATE_SLEEP_SECONDS:
            time.sleep(DATA_RATE_SLEEP_SECONDS)
        return row, 0, 0
    except meraki.APIError as exc:
        row = {c: None for c in cols}
        row["serial"] = serial
        context.log.warning(f"Data rate fetch failed for {serial}: {exc.status}")
        return row, 1, 0


def _fetch_data_rate_df(
    db: meraki.DashboardAPI,
    inventory: pd.DataFrame,
    t0: str,
    t1: str,
    context: OpExecutionContext,
) -> pd.DataFrame:
    cols = ["serial", "download_mbps", "upload_mbps", "avg_mbps"]
    if len(inventory) == 0:
        return pd.DataFrame(columns=cols)

    unsupported_networks: set[str] = set()
    for network_id, grp in inventory.groupby("networkId", sort=False):
        sample_serial = str(grp.iloc[0]["serial"])
        if not _probe_data_rate_network(db, str(network_id), sample_serial, t0, t1):
            unsupported_networks.add(str(network_id))
            net_name = grp.iloc[0].get("networkName", network_id)
            context.log.warning(
                f"Data rate unavailable for network {net_name} ({network_id}): requires MR 27.0+"
            )
        if DATA_RATE_SLEEP_SECONDS:
            time.sleep(DATA_RATE_SLEEP_SECONDS)

    rows: list[dict[str, Any]] = []
    failed = 0
    skipped = 0
    for _, ap in inventory.iterrows():
        row, f, s = _fetch_ap_data_rate_row(
            db, ap, unsupported_networks, cols, t0, t1, context
        )
        rows.append(row)
        failed += f
        skipped += s

    if skipped:
        context.log.info(
            f"data rate: {skipped} AP(s) on unsupported networks (MR 27.0+)"
        )
    if failed:
        context.log.warning(f"data rate: {failed} AP(s) failed")

    return pd.DataFrame(rows)


def _fetch_latency_history_df(
    db: meraki.DashboardAPI,
    inventory: pd.DataFrame,
    t0: str,
    t1: str,
    context: OpExecutionContext,
) -> pd.DataFrame:
    cols = ["serial", "avg_latency_ms"]
    if len(inventory) == 0:
        return pd.DataFrame(columns=cols)

    rows: list[dict[str, Any]] = []
    failed = 0
    for _, ap in inventory.iterrows():
        network_id = str(ap["networkId"])
        serial = str(ap["serial"])
        try:
            buckets = call(
                db.wireless.getNetworkWirelessLatencyHistory,
                network_id,
                t0=t0,
                t1=t1,
                resolution=DAY_RESOLUTION_SECONDS,
                deviceSerial=serial,
            )
            rows.append(
                {
                    "serial": serial,
                    "avg_latency_ms": _mean_numeric(buckets, "avgLatencyMs"),
                }
            )
            if LATENCY_HISTORY_SLEEP_SECONDS:
                time.sleep(LATENCY_HISTORY_SLEEP_SECONDS)
        except meraki.APIError as exc:
            failed += 1
            rows.append({"serial": serial, "avg_latency_ms": None})
            context.log.warning(
                f"Latency history fetch failed for {serial}: {exc.status}"
            )

    if failed:
        context.log.warning(f"latency history: {failed} AP(s) failed")

    return pd.DataFrame(rows)


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
        frames.append(_normalize_packet_loss(raw))
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


def build_combined_dataframe(
    report_day: dt.date,
    spark_session: SparkSession,
    context: OpExecutionContext,
) -> pd.DataFrame:
    db = create_dashboard(settings.VCT_MERAKI_API_KEY)
    day_t0, day_t1 = _day_window_iso(report_day)

    changes_df = _fetch_day_change_history(db, day_t0, day_t1)
    events_df = _build_events_from_changes(
        changes_df, report_day, spark_session, context
    )

    if len(events_df) == 0:
        return pd.DataFrame(columns=COMBINED_COLUMNS)

    inventory = _build_ap_inventory(db, context)

    uptime_source = (
        changes_df[["serial", "occurredAt", "previousStatus", "newStatus"]].copy()
        if len(changes_df) > 0
        else pd.DataFrame()
    )
    uptime_df = _compute_uptime_metrics(uptime_source, day_t0, day_t1)
    packet_loss_df = _fetch_packet_loss(db, day_t0, day_t1)

    context.log.info("Fetching latency history per AP...")
    latency_df = _fetch_latency_history_df(db, inventory, day_t0, day_t1, context)

    context.log.info("Fetching data rate per AP...")
    data_rate_df = _fetch_data_rate_df(db, inventory, day_t0, day_t1, context)

    collected_at = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")

    qos_df = inventory.merge(uptime_df, on="serial", how="left", validate="1:1")
    qos_df = qos_df.merge(packet_loss_df, on="serial", how="left", validate="1:1")
    qos_df = qos_df.merge(latency_df, on="serial", how="left", validate="1:1")
    qos_df = qos_df.merge(data_rate_df, on="serial", how="left", validate="1:1")
    qos_df["window_end"] = day_t1
    qos_df["collected_at"] = collected_at

    qos_cols = ["serial"] + QOS_DAY_COLUMNS
    qos_subset = qos_df[[c for c in qos_cols if c in qos_df.columns]].rename(
        columns={"serial": "device_id"}
    )

    combined = events_df.merge(qos_subset, on="device_id", how="left", validate="m:1")

    for col in COMBINED_COLUMNS:
        if col not in combined.columns:
            combined[col] = None

    return combined[COMBINED_COLUMNS]
