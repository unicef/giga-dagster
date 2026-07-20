"""Schema definitions and type-enforcement utilities for VCT Meraki outputs."""

from __future__ import annotations

import io

import pandas as pd


def _cast_column(series: pd.Series, dtype: str) -> pd.Series:
    if dtype == "string":
        return series.where(series.isna(), series.astype(str))
    if dtype == "timestamp":
        return pd.to_datetime(series, utc=True, errors="coerce")
    if dtype == "date":
        return pd.to_datetime(series, errors="coerce").dt.date
    if dtype == "integer":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if dtype == "float":
        return pd.to_numeric(series, errors="coerce").astype("float64")
    return series


def enforce_schema(df: pd.DataFrame, schema: dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    for col, dtype in schema.items():
        if col in df.columns:
            df[col] = _cast_column(df[col], dtype)
    return df


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


QOS_60MIN_SCHEMA: dict[str, str] = {
    "device_id": "string",
    "deviceName": "string",
    "model": "string",
    "productType": "string",
    "networkId": "string",
    "networkName": "string",
    "organization": "string",
    "meraki_name_room": "string",
    "downstream_total_packet": "integer",
    "downstream_packet_lost": "integer",
    "downstream_loss_pct": "float",
    "avg_latency_ms": "float",
    "download_mbps": "float",
    "upload_mbps": "float",
    "avg_mbps": "float",
    "downtime_s": "float",
    "uptime_percentage": "float",
    "time_window": "string",
    "timestamp": "timestamp",
    "window_end": "timestamp",
    "timespan_s": "integer",
    "collected_at": "timestamp",
    "school_id_govt": "string",
    "school_id_giga": "string",
}

QOS_AVAILABILITY_SCHEMA: dict[str, str] = {
    "device_id": "string",
    "deviceName": "string",
    "model": "string",
    "productType": "string",
    "networkId": "string",
    "networkName": "string",
    "organization": "string",
    "meraki_name_room": "string",
    "timestamp": "timestamp",
    "five_min_window": "string",
    "previousStatus": "string",
    "newStatus": "string",
    "count": "integer",
    "date": "date",
    "school_id_govt": "string",
    "school_id_giga": "string",
}

QOS_COMBINED_SCHEMA: dict[str, str] = {
    **QOS_AVAILABILITY_SCHEMA,
    "downstream_total_packet": "integer",
    "downstream_packet_lost": "integer",
    "downstream_loss_pct": "float",
    "avg_latency_ms": "float",
    "download_mbps": "float",
    "upload_mbps": "float",
    "avg_mbps": "float",
    "downtime_s": "float",
    "uptime_percentage": "float",
    "window_end": "timestamp",
    "collected_at": "timestamp",
}
