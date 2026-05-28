"""
Utility for generating interactive school maps using Folium.
"""

import folium
import pandas as pd
from folium.plugins import MarkerCluster

from dagster import OpExecutionContext

PASSED_COLOR = "#28a745"
FAILED_COLOR = "#dc3545"

UNINHABITED_LABEL_HINTS = ("uninhabited",)


def _coerce_coords(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or not {"latitude", "longitude"}.issubset(df.columns):
        return df.iloc[0:0].copy()

    df = df.copy()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    return df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)]


def _find_column(df: pd.DataFrame, hints: tuple[str, ...]) -> str | None:
    """Find the first column whose lower-cased name contains any of ``hints``."""
    if df.empty:
        return None
    for col in df.columns:
        lc = col.lower()
        if any(h in lc for h in hints):
            return col
    return None


def _get_map_bounds(
    passed_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    context: OpExecutionContext,
) -> tuple[float, float, list[list[float]] | None]:
    all_lats = passed_df["latitude"].tolist() + failed_df["latitude"].tolist()
    all_lons = passed_df["longitude"].tolist() + failed_df["longitude"].tolist()

    if not all_lats:
        context.log.warning("No location data available for map generation")
        return 0.0, 0.0, None

    return (
        sum(all_lats) / len(all_lats),
        sum(all_lons) / len(all_lons),
        [
            [min(all_lats), min(all_lons)],
            [max(all_lats), max(all_lons)],
        ],
    )


def _fmt(value) -> str:
    """Render a popup value, treating NaN / None / empty as 'N/A'."""
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    s = str(value).strip()
    return s if s else "N/A"


def _fmt_int(value) -> str:
    """Format population counts with thousands separators."""
    formatted = _fmt(value)
    if formatted == "N/A":
        return formatted
    try:
        return f"{int(float(formatted)):,}"
    except (TypeError, ValueError):
        return formatted


def _build_popup(
    row: dict,
    status: str,
    uninhabited_col: str | None,
    include_failure_reason: bool,
) -> str:
    parts = [
        f"<b>School:</b> {_fmt(row.get('school_name'))}",
        f"<b>Status:</b> {status}",
        f"<b>Rural / Urban:</b> {_fmt(row.get('rurban_detected'))}",
        (
            "<b>Population within:</b><br>"
            f"&nbsp;&nbsp;1 km: {_fmt_int(row.get('pop_within_1km'))}<br>"
            f"&nbsp;&nbsp;2 km: {_fmt_int(row.get('pop_within_2km'))}<br>"
            f"&nbsp;&nbsp;3 km: {_fmt_int(row.get('pop_within_3km'))}"
        ),
    ]
    if uninhabited_col is not None:
        parts.append(f"<b>In Uninhabited Area:</b> {_fmt(row.get(uninhabited_col))}")
    if include_failure_reason:
        parts.append(f"<b>Reason:</b> {_fmt(row.get('failure_reason'))}")
    return "<br>".join(parts)


def _add_school_markers(
    df: pd.DataFrame,
    cluster: MarkerCluster,
    color: str,
    status: str,
    uninhabited_col: str | None,
    include_failure_reason: bool = False,
) -> int:
    if df.empty:
        return 0
    # to_dict("records") preserves original column names (incl. spaces / special
    # chars from NocoDB human-readable labels), unlike itertuples which mangles
    # them into Python identifiers.
    records = df.to_dict("records")
    for row in records:
        popup_html = _build_popup(
            row,
            status=status,
            uninhabited_col=uninhabited_col,
            include_failure_reason=include_failure_reason,
        )
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            popup=folium.Popup(popup_html, max_width=320),
        ).add_to(cluster)

    return len(records)


def _add_filter_layer(
    m: folium.Map,
    df: pd.DataFrame,
    name: str,
    color: str,
    show: bool,
    uninhabited_col: str | None,
    include_failure_reason: bool,
    status: str,
) -> int:
    """Add a hidden-by-default filter layer with its own cluster."""
    if df.empty:
        return 0
    cluster = MarkerCluster(name=name, overlay=True, control=True, show=show)
    count = _add_school_markers(
        df,
        cluster,
        color=color,
        status=status,
        uninhabited_col=uninhabited_col,
        include_failure_reason=include_failure_reason,
    )
    cluster.add_to(m)
    return count


def _is_yes(series: pd.Series) -> pd.Series:
    """Truthy mask for the Yes/No human-readable flags."""
    return series.astype(str).str.strip().str.lower().eq("yes")


def generate_school_map_html(
    country_code: str,
    passed_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    context: OpExecutionContext,
) -> str:
    """Generate interactive HTML map with passed/failed schools."""
    passed_df = _coerce_coords(passed_df)
    failed_df = _coerce_coords(failed_df)
    center_lat, center_lon, bounds = _get_map_bounds(passed_df, failed_df, context)

    detection_source = passed_df if not passed_df.empty else failed_df
    uninhabited_col = _find_column(detection_source, UNINHABITED_LABEL_HINTS)

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        control_scale=True,
    )

    if bounds:
        m.fit_bounds(bounds, padding=[50, 50])

    # --- Default layers: all passed / all failed --------------------------
    passed_cluster = MarkerCluster(
        name="Schools Passed", overlay=True, control=True, show=True
    )
    failed_cluster = MarkerCluster(
        name="Schools Rejected", overlay=True, control=True, show=True
    )

    passed_count = _add_school_markers(
        passed_df,
        passed_cluster,
        color=PASSED_COLOR,
        status="Passed",
        uninhabited_col=uninhabited_col,
    )
    failed_count = _add_school_markers(
        failed_df,
        failed_cluster,
        color=FAILED_COLOR,
        status="Failed",
        uninhabited_col=uninhabited_col,
        include_failure_reason=True,
    )
    passed_cluster.add_to(m)
    failed_cluster.add_to(m)

    # --- Filter layers (default hidden) -----------------------------------
    combined_df = pd.concat([passed_df, failed_df], ignore_index=True)
    if not combined_df.empty:
        rurban = (
            combined_df.get("rurban_detected", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.strip()
        )
        urban_df = combined_df[rurban.str.lower() == "urban"]
        rural_df = combined_df[rurban.str.lower() == "rural"]
        unknown_df = combined_df[~rurban.str.lower().isin(["urban", "rural"])]

        _add_filter_layer(
            m,
            urban_df,
            name="Filter: Urban",
            color="#0d6efd",
            show=False,
            uninhabited_col=uninhabited_col,
            include_failure_reason=False,
            status="—",
        )
        _add_filter_layer(
            m,
            rural_df,
            name="Filter: Rural",
            color="#fd7e14",
            show=False,
            uninhabited_col=uninhabited_col,
            include_failure_reason=False,
            status="—",
        )
        _add_filter_layer(
            m,
            unknown_df,
            name="Filter: Rurality Unknown",
            color="#6c757d",
            show=False,
            uninhabited_col=uninhabited_col,
            include_failure_reason=False,
            status="—",
        )

        if uninhabited_col is not None:
            uninhabited_df = combined_df[_is_yes(combined_df[uninhabited_col])]
            _add_filter_layer(
                m,
                uninhabited_df,
                name="Filter: In Uninhabited Area",
                color="#6f42c1",
                show=False,
                uninhabited_col=uninhabited_col,
                include_failure_reason=False,
                status="—",
            )

    folium.LayerControl(collapsed=False).add_to(m)

    context.log.info(
        f"Added {passed_count} passed schools and {failed_count} failed schools to map "
        f"(uninhabited_col={uninhabited_col!r})"
    )

    return m.get_root().render()
