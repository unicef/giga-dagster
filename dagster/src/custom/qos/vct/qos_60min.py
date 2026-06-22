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


def _align_window_end_utc(
    now: Optional[dt.datetime] = None, bucket_seconds: int = 300
) -> dt.datetime:
    now = now or dt.datetime.now(dt.UTC)
    now = now.replace(second=0, microsecond=0)
    minute = (now.minute // (bucket_seconds // 60)) * (bucket_seconds // 60)
    return now.replace(minute=minute)


def _snapshot_window(timespan_s: int) -> tuple[dt.datetime, dt.datetime, str, str]:
    end = _align_window_end_utc(bucket_seconds=timespan_s)
    start = end - dt.timedelta(seconds=timespan_s)
    t0 = start.isoformat().replace("+00:00", "Z")
    t1 = end.isoformat().replace("+00:00", "Z")
    return start, end, t0, t1


def _format_time_window(bucket_start: dt.datetime, bucket_minutes: int) -> str:
    if bucket_start.tzinfo is None:
        bucket_start = bucket_start.replace(tzinfo=dt.UTC)
    else:
        bucket_start = bucket_start.astimezone(dt.UTC)
    bucket_start = bucket_start.replace(second=0, microsecond=0)
    minute = (bucket_start.minute // bucket_minutes) * bucket_minutes
    bucket_start = bucket_start.replace(minute=minute)
    bucket_end = bucket_start + dt.timedelta(minutes=bucket_minutes)
    return f"{bucket_start.hour:02d}.{bucket_start.minute:02d} - {bucket_end.hour:02d}.{bucket_end.minute:02d}"


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
    if len(df) == 0:
        return df
    df["occurredAt"] = pd.to_datetime(df["occurredAt"], utc=True, errors="coerce")
    df = df.dropna(subset=["serial", "occurredAt"])
    df = df[df["previousStatus"] != df["newStatus"]].copy()
    return df.sort_values(["serial", "occurredAt"]).reset_index(drop=True)


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
    cols = ["serial", "downtime_s", "uptime_pct"]
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
        down_s, uptime_pct = result
        results.append(
            {"serial": serial, "downtime_s": down_s, "uptime_pct": uptime_pct}
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


def _fetch_change_history_for_org(
    db: meraki.DashboardAPI, org_id: str, network_ids: list[str], timespan: int
) -> list:
    return call(
        db.organizations.getOrganizationDevicesAvailabilitiesChangeHistory,
        org_id,
        networkIds=network_ids,
        productTypes=["wireless"],
        timespan=timespan,
        total_pages="all",
    )


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
                "loss_pct": downstream.get("lossPercentage"),
            }
        )
    cols = ["serial", "downstream_total_packet", "downstream_packet_lost", "loss_pct"]
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return pd.DataFrame(columns=cols)
    return df.drop_duplicates(subset=["serial"], keep="first")


def _fetch_packet_loss_for_org(
    db: meraki.DashboardAPI, org_id: str, network_ids: list[str], timespan: int
) -> pd.DataFrame:
    if len(network_ids) == 0:
        return pd.DataFrame()
    raw = call(
        db.wireless.getOrganizationWirelessDevicesPacketLossByDevice,
        org_id,
        networkIds=network_ids,
        timespan=timespan,
        total_pages="all",
    )
    return _normalize_packet_loss(raw)


def _normalize_data_rate_history(
    serial: str, buckets: Optional[list]
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "serial": serial,
        "download_kbps": None,
        "upload_kbps": None,
        "avg_kbps": None,
    }
    if not buckets:
        return out
    row = buckets[0]
    out["download_kbps"] = row.get("downloadKbps")
    out["upload_kbps"] = row.get("uploadKbps")
    out["avg_kbps"] = row.get("averageKbps")
    return out


def _data_rate_network_unsupported(exc: meraki.APIError) -> bool:
    if exc.status != 400:
        return False
    msg = str(getattr(exc, "message", "") or exc)
    return "MR 27.0" in msg


def _probe_data_rate_network(
    db: meraki.DashboardAPI, network_id: str, sample_serial: str
) -> bool:
    try:
        call(
            db.wireless.getNetworkWirelessDataRateHistory,
            network_id,
            timespan=DATA_RATE_TIMESPAN_SECONDS,
            resolution=DATA_RATE_RESOLUTION_SECONDS,
            deviceSerial=sample_serial,
        )
        return True
    except meraki.APIError as exc:
        if _data_rate_network_unsupported(exc):
            return False
        raise


def _fetch_data_rate_for_ap(
    db: meraki.DashboardAPI, network_id: str, serial: str
) -> dict[str, Any]:
    buckets = call(
        db.wireless.getNetworkWirelessDataRateHistory,
        network_id,
        timespan=DATA_RATE_TIMESPAN_SECONDS,
        resolution=DATA_RATE_RESOLUTION_SECONDS,
        deviceSerial=serial,
    )
    return _normalize_data_rate_history(serial, buckets)


def _fetch_ap_data_rate_row(
    db: meraki.DashboardAPI,
    ap: Any,
    unsupported_networks: set[str],
    cols: list[str],
    context: OpExecutionContext,
) -> tuple[dict[str, Any], int, int]:
    network_id = str(ap["networkId"])
    serial = str(ap["serial"])
    if network_id in unsupported_networks:
        row = {c: None for c in cols}
        row["serial"] = serial
        return row, 0, 1
    try:
        row = _fetch_data_rate_for_ap(db, network_id, serial)
        if DATA_RATE_SLEEP_SECONDS:
            time.sleep(DATA_RATE_SLEEP_SECONDS)
        return row, 0, 0
    except meraki.APIError as exc:
        context.log.warning(f"Data rate fetch failed for {serial}: {exc.status}")
        row = {c: None for c in cols}
        row["serial"] = serial
        return row, 1, 0


def _fetch_data_rate_df(
    db: meraki.DashboardAPI,
    inventory: pd.DataFrame,
    context: OpExecutionContext,
) -> pd.DataFrame:
    cols = ["serial", "download_kbps", "upload_kbps", "avg_kbps"]
    if len(inventory) == 0:
        return pd.DataFrame(columns=cols)

    unsupported_networks: set[str] = set()
    for network_id, grp in inventory.groupby("networkId", sort=False):
        sample_serial = str(grp.iloc[0]["serial"])
        if not _probe_data_rate_network(db, str(network_id), sample_serial):
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
        row, f, s = _fetch_ap_data_rate_row(db, ap, unsupported_networks, cols, context)
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


def _avg_latency_ms_from_buckets(buckets: Optional[list]) -> Any:
    if not buckets:
        return None
    return buckets[0].get("avgLatencyMs")


def _normalize_latency_history(
    serial: str, aggregate_buckets: Optional[list]
) -> dict[str, Any]:
    return {
        "serial": serial,
        "avg_latency_ms": _avg_latency_ms_from_buckets(aggregate_buckets),
    }


def _fetch_latency_history_for_ap(
    db: meraki.DashboardAPI, network_id: str, serial: str
) -> dict[str, Any]:
    aggregate = call(
        db.wireless.getNetworkWirelessLatencyHistory,
        network_id,
        timespan=LATENCY_HISTORY_TIMESPAN_SECONDS,
        resolution=LATENCY_HISTORY_RESOLUTION_SECONDS,
        deviceSerial=serial,
    )
    return _normalize_latency_history(serial, aggregate)


def _fetch_latency_history_df(
    db: meraki.DashboardAPI,
    inventory: pd.DataFrame,
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
            rows.append(_fetch_latency_history_for_ap(db, network_id, serial))
            if LATENCY_HISTORY_SLEEP_SECONDS:
                time.sleep(LATENCY_HISTORY_SLEEP_SECONDS)
        except meraki.APIError as exc:
            failed += 1
            row = {c: None for c in cols}
            row["serial"] = serial
            rows.append(row)
            context.log.warning(
                f"Latency history fetch failed for {serial}: {exc.status}"
            )

    if failed:
        context.log.warning(f"latency history: {failed} AP(s) failed")

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


def build_snapshot_dataframe(
    spark_session: SparkSession,
    context: OpExecutionContext,
) -> tuple[pd.DataFrame, dt.datetime, dt.datetime]:
    db = create_dashboard(settings.VCT_MERAKI_API_KEY)
    window_start, window_end, t0, t1 = _snapshot_window(TIMESPAN_SECONDS)
    context.log.info(f"Window: {t0} → {t1}")

    inventory = _build_ap_inventory(db, context)
    context.log.info(f"Inventory: {len(inventory)} APs")
    if len(inventory) == 0:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS), window_start, window_end

    org_items = list(NETWORK_DICT.items())

    changes_frames: list[pd.DataFrame] = []
    for org_id, network_ids in org_items:
        raw = _fetch_change_history_for_org(db, org_id, network_ids, TIMESPAN_SECONDS)
        changes_frames.append(_normalize_changes(raw))
    changes_df = (
        pd.concat(changes_frames, ignore_index=True)
        if changes_frames
        else pd.DataFrame()
    )

    packet_loss_frames: list[pd.DataFrame] = []
    for org_id, network_ids in org_items:
        packet_loss_frames.append(
            _fetch_packet_loss_for_org(db, org_id, network_ids, TIMESPAN_SECONDS)
        )
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
    latency_hist_df = _fetch_latency_history_df(db, inventory, context)

    context.log.info("Fetching data rate per AP...")
    data_rate_df = _fetch_data_rate_df(db, inventory, context)

    uptime_df = _compute_uptime_metrics(changes_df, t0, t1)
    collected_at = dt.datetime.now(dt.UTC)

    out = inventory.merge(uptime_df, on="serial", how="left", validate="1:1")
    out = out.merge(packet_loss_df, on="serial", how="left", validate="1:1")
    out = out.merge(latency_hist_df, on="serial", how="left", validate="1:1")
    out = out.merge(data_rate_df, on="serial", how="left", validate="1:1")
    out["time_window"] = _format_time_window(
        window_start, bucket_minutes=max(1, TIMESPAN_SECONDS // 60)
    )
    out["window_start"] = t0
    out["window_end"] = t1
    out["timespan_s"] = TIMESPAN_SECONDS
    out["collected_at"] = collected_at.isoformat().replace("+00:00", "Z")
    out = _attach_school_ids(out, spark_session, context)

    for col in SNAPSHOT_COLUMNS:
        if col not in out.columns:
            out[col] = None

    return out[SNAPSHOT_COLUMNS], window_start, window_end
