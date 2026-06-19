"""VCT Meraki daily events — helpers and data-fetch logic."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

import pandas as pd
from pyspark.sql import (
    SparkSession,
    functions as F,
)
from src.custom.qos.meraki_client import call, create_dashboard
from src.custom.qos.vct.constants import NETWORK_DICT, ORG_NAMES
from src.settings import settings

from dagster import OpExecutionContext

FIVE_MIN_BUCKET_MINUTES = 5

EVENT_COLUMNS = [
    "serial",
    "deviceName",
    "model",
    "productType",
    "networkId",
    "networkName",
    "organization",
    "meraki_name_room",
    "occurredAt",
    "five_min_window",
    "previousStatus",
    "newStatus",
    "changes_count_day",
    "report_date",
    "school_id_govt",
    "school_id_giga",
]


def _day_window_iso(day: dt.date) -> tuple[str, str]:
    t0 = dt.datetime(day.year, day.month, day.day, tzinfo=dt.UTC)
    t1 = t0 + dt.timedelta(days=1)
    return t0.isoformat().replace("+00:00", "Z"), t1.isoformat().replace("+00:00", "Z")


def _format_five_min_window(
    bucket_start: dt.datetime, bucket_minutes: int = FIVE_MIN_BUCKET_MINUTES
) -> str:
    if bucket_start.tzinfo is None:
        bucket_start = bucket_start.replace(tzinfo=dt.UTC)
    else:
        bucket_start = bucket_start.astimezone(dt.UTC)
    bucket_start = bucket_start.replace(second=0, microsecond=0)
    minute = (bucket_start.minute // bucket_minutes) * bucket_minutes
    bucket_start = bucket_start.replace(minute=minute)
    bucket_end = bucket_start + dt.timedelta(minutes=bucket_minutes)
    return f"{bucket_start.hour:02d}.{bucket_start.minute:02d} - {bucket_end.hour:02d}.{bucket_end.minute:02d}"


def _add_five_min_window_from_timestamp(
    series: pd.Series, bucket_minutes: int = FIVE_MIN_BUCKET_MINUTES
) -> pd.Series:
    ts = pd.to_datetime(series, utc=True)

    def label(t: pd.Timestamp) -> Optional[str]:
        if pd.isna(t):
            return None
        return _format_five_min_window(t.to_pydatetime(), bucket_minutes=bucket_minutes)

    return ts.map(label)


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
    side_list = details.get(which) or []
    for item in side_list:
        if item.get("name") == key:
            return item.get("value")
    return None


def _normalize_changes(raw_rows: Optional[list]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for r in raw_rows or []:
        dev = r.get("device") or {}
        net = r.get("network") or {}
        det = r.get("details") or {}
        occurred_at = r.get("ts") or r.get("time") or r.get("occurredAt")
        rows.append(
            {
                "serial": dev.get("serial"),
                "deviceName": dev.get("name"),
                "model": dev.get("model"),
                "productType": dev.get("productType"),
                "networkId": net.get("id"),
                "networkName": net.get("name"),
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
    return df.merge(lookup, on="serial", how="left", validate="m:1")


def build_events_dataframe(
    report_day: dt.date,
    spark_session: SparkSession,
    context: OpExecutionContext,
) -> pd.DataFrame:
    db = create_dashboard(settings.VCT_MERAKI_API_KEY)
    day_t0, day_t1 = _day_window_iso(report_day)
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
        if len(df) > 0:
            day_mask = (df["occurredAt"] >= day_start) & (df["occurredAt"] < day_end)
            df = df.loc[day_mask].copy()
            if len(df) > 0:
                df["organization"] = org_label
                df["meraki_name_room"] = df["deviceName"].map(_parse_meraki_name_room)
        frames.append(df)

    events_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    events_df["report_date"] = report_day.isoformat()

    if len(events_df) > 0 and "occurredAt" in events_df.columns:
        events_df["five_min_window"] = _add_five_min_window_from_timestamp(
            events_df["occurredAt"]
        )
        events_df["changes_count_day"] = events_df.groupby("serial", sort=False)[
            "serial"
        ].transform("count")

    events_df = _attach_school_ids(events_df, spark_session, context)

    for col in EVENT_COLUMNS:
        if col not in events_df.columns:
            events_df[col] = None

    return events_df[EVENT_COLUMNS]
