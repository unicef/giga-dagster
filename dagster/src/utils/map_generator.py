"""
Utility for generating interactive school maps using Folium.
"""

import folium
import pandas as pd
from folium.plugins import MarkerCluster

from dagster import OpExecutionContext

PASSED_COLOR = "#28a745"
FAILED_COLOR = "#dc3545"

# Raw integer DQ column written by dq_geolocation_extract_relevant_columns
_UNINHABITED_COL = "dq_is_in_uninhabited_area"
_DUP_FLAG_COL = "dq_duplicate_group_flag_50m"
_DUP_COUNT_COL = "dq_duplicate_group_count_50m"
_DUP_GROUP_COL = "dq_duplicate_group_id_50m"


def _coerce_coords(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or not {"latitude", "longitude"}.issubset(df.columns):
        return df.iloc[0:0].copy()

    df = df.copy()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    return df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)]


def _get_map_bounds(
    passed_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    context: OpExecutionContext,
) -> tuple[float, float, list[list[float]] | None]:
    all_lats = (
        passed_df["latitude"].tolist() if "latitude" in passed_df.columns else []
    ) + (failed_df["latitude"].tolist() if "latitude" in failed_df.columns else [])
    all_lons = (
        passed_df["longitude"].tolist() if "longitude" in passed_df.columns else []
    ) + (failed_df["longitude"].tolist() if "longitude" in failed_df.columns else [])

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


def _flag(value) -> str:
    """Convert raw int DQ flag (1/0/None) to true/false string."""
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except (TypeError, ValueError):
        pass
    try:
        return "true" if int(float(value)) == 1 else "false"
    except (TypeError, ValueError):
        return "N/A"


def _build_popup(
    row: dict,
    status: str,
    include_failure_reason: bool,
) -> str:
    admin1 = _fmt(row.get("admin1"))
    admin1_id = _fmt(row.get("admin1_id_giga"))
    admin2 = _fmt(row.get("admin2"))
    admin2_id = _fmt(row.get("admin2_id_giga"))

    parts = [
        f"<b>school_id_giga</b>: {_fmt(row.get('school_id_giga'))}",
        f"<b>school_id_govt</b>: {_fmt(row.get('school_id_govt'))}",
        f"<b>latitude</b>: {_fmt(row.get('latitude'))}",
        f"<b>longitude</b>: {_fmt(row.get('longitude'))}",
        f"<b>school_name</b>: {_fmt(row.get('school_name'))}",
        f"<b>education_level</b>: {_fmt(row.get('education_level'))}",
        f"<b>admin1</b>: {admin1} ({admin1_id})",
        f"<b>admin2</b>: {admin2} ({admin2_id})",
        f"<b>rurban</b>: {_fmt(row.get('rurban_detected'))}",
        f"<b>uninhabited</b>: {_flag(row.get(_UNINHABITED_COL))}",
        f"<b>duplicate_50_flag</b>: {_flag(row.get(_DUP_FLAG_COL))}",
        f"<b>duplicate_50_group_id</b>: {_fmt(row.get(_DUP_GROUP_COL))}",
        f"<b>duplicate_50_count</b>: {_fmt(row.get(_DUP_COUNT_COL))}",
        f"<b>Status</b>: {status}",
    ]
    if include_failure_reason:
        parts.append(f"<b>Reason:</b> {_fmt(row.get('failure_reason'))}")
    return "<br>".join(parts)


def _add_school_markers(
    df: pd.DataFrame,
    cluster: MarkerCluster,
    color: str,
    status: str,
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
            include_failure_reason=include_failure_reason,
        )
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            popup=folium.Popup(popup_html, max_width=380),
        ).add_to(cluster)

    return len(records)


def _add_filter_layer(
    m: folium.Map,
    df: pd.DataFrame,
    name: str,
    color: str,
    show: bool,
) -> int:
    """Add a hidden-by-default filter layer with its own cluster.

    The layer name includes the row count automatically.
    Markers inherit the real pass / fail colour and status from the
    ``_dq_status`` and ``_dq_color`` columns that must be present in ``df``.
    """
    count = len(df)
    if df.empty:
        return 0
    cluster = MarkerCluster(
        name=f"{name} ({count})", overlay=True, control=True, show=show
    )
    records = df.to_dict("records")
    for row in records:
        row_color = row.get("_dq_color", color)
        row_status = row.get("_dq_status", "—")
        popup_html = _build_popup(
            row,
            status=row_status,
            include_failure_reason="failure_reason" in row,
        )
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color=row_color,
            fill=True,
            fillColor=row_color,
            fillOpacity=0.7,
            popup=folium.Popup(popup_html, max_width=380),
        ).add_to(cluster)
    cluster.add_to(m)
    return len(records)


def _dq_int(series: pd.Series) -> pd.Series:
    """Coerce a raw dq int column (possibly float / object) to int, NaN→0."""
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


def generate_school_map_html(
    country_code: str,
    passed_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    context: OpExecutionContext,
) -> str:
    """Generate interactive HTML map with passed/failed schools."""
    context.log.info(
        f"Map generation input: passed={len(passed_df)}, failed={len(failed_df)}"
    )
    context.log.info(
        f"Passed columns: {list(passed_df.columns) if not passed_df.empty else []}"
    )
    context.log.info(
        f"Failed columns: {list(failed_df.columns) if not failed_df.empty else []}"
    )

    passed_df = _coerce_coords(passed_df)
    failed_df = _coerce_coords(failed_df)

    context.log.info(
        f"After coordinate coercion: passed={len(passed_df)}, failed={len(failed_df)}"
    )
    center_lat, center_lon, bounds = _get_map_bounds(passed_df, failed_df, context)

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        control_scale=True,
    )

    if bounds:
        m.fit_bounds(bounds, padding=[50, 50])

    # --- Default layers: all passed / all failed --------------------------
    passed_cluster = MarkerCluster(
        name=f"Schools Passed ({len(passed_df)})", overlay=True, control=True, show=True
    )
    failed_cluster = MarkerCluster(
        name=f"Schools Failed ({len(failed_df)})", overlay=True, control=True, show=True
    )

    passed_count = _add_school_markers(
        passed_df,
        passed_cluster,
        color=PASSED_COLOR,
        status="Passed",
    )
    failed_count = _add_school_markers(
        failed_df,
        failed_cluster,
        color=FAILED_COLOR,
        status="Failed",
        include_failure_reason=True,
    )
    passed_cluster.add_to(m)
    failed_cluster.add_to(m)

    # --- Filter layers (default hidden) -----------------------------------
    # Tag each row with its real status and colour so filter-layer popups
    # show the same information as the main passed/failed layers.
    passed_tagged = passed_df.copy()
    passed_tagged["_dq_status"] = "Passed"
    passed_tagged["_dq_color"] = PASSED_COLOR

    failed_tagged = failed_df.copy()
    failed_tagged["_dq_status"] = "Failed"
    failed_tagged["_dq_color"] = FAILED_COLOR

    combined_df = pd.concat([passed_tagged, failed_tagged], ignore_index=True)

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

        _add_filter_layer(m, urban_df, name="Urban", color="#0d6efd", show=False)
        _add_filter_layer(m, rural_df, name="Rural", color="#fd7e14", show=False)
        _add_filter_layer(
            m, unknown_df, name="Rurality Unknown", color="#6c757d", show=False
        )

        # Uninhabited filter: dq_is_in_uninhabited_area == 1 means IS uninhabited
        if _UNINHABITED_COL in combined_df.columns:
            uninhabited_df = combined_df[_dq_int(combined_df[_UNINHABITED_COL]) == 1]
            _add_filter_layer(
                m,
                uninhabited_df,
                name="In Uninhabited Area",
                color="#6f42c1",
                show=False,
            )

    folium.LayerControl(collapsed=False).add_to(m)

    context.log.info(
        f"Map generation complete: {passed_count} passed schools and {failed_count} failed schools added."
    )

    return m.get_root().render()
