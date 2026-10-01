"""Generic schema-casting and serialization helpers shared across QoS country integrations."""

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


def enforce_prd_schema(df: pd.DataFrame, schema: dict[str, str]) -> pd.DataFrame:
    """Like enforce_schema, but raises on any missing or unexpected column instead
    of silently skipping it - matches the qos_scripts enforce_prd_schema() contract
    this ports: don't write/upload a schema-drifted output, fail the run instead."""
    actual = set(df.columns)
    expected = set(schema)
    missing = expected - actual
    extra = actual - expected
    if missing or extra:
        raise ValueError(
            f"output does not match expected schema: missing={sorted(missing)} extra={sorted(extra)}"
        )
    return enforce_schema(df[list(schema)], schema)


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()
