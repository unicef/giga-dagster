"""
Utility for generating interactive school maps using Folium.
"""

import folium
import pandas as pd
from folium.plugins import MarkerCluster
from jinja2 import BaseLoader, Environment

from dagster import OpExecutionContext

PASSED_COLOR = "#28a745"
FAILED_COLOR = "#dc3545"

_UNINHABITED_COL = "dq_is_in_uninhabited_area"
_DUP_FLAG_COL = "dq_duplicate_group_flag_50m"
_DUP_COUNT_COL = "dq_duplicate_group_count_50m"
_DUP_GROUP_COL = "dq_duplicate_group_id_50m"

_POPUP_TEMPLATE = Environment(
    loader=BaseLoader(),
    autoescape=True,
).from_string(
    """
{%- for label, value in fields -%}
<b>{{ label }}</b>: {{ value }}{% if not loop.last %}<br>{% endif %}
{%- endfor -%}
"""
)


def _filter_rows_with_valid_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows with numeric latitude/longitude inside valid coordinate ranges."""
    if df.empty:
        return df.copy()

    if not {"latitude", "longitude"}.issubset(df.columns):
        return df.iloc[0:0].copy()

    lat = pd.to_numeric(df["latitude"], errors="coerce")
    lon = pd.to_numeric(df["longitude"], errors="coerce")

    valid_coordinate_mask = (
        lat.notna() & lon.notna() & lat.between(-90, 90) & lon.between(-180, 180)
    )

    filtered_df = df[valid_coordinate_mask].copy()
    filtered_df["latitude"] = lat[valid_coordinate_mask]
    filtered_df["longitude"] = lon[valid_coordinate_mask]
    return filtered_df


def _calculate_coordinate_bounds(
    school_coordinates_df: pd.DataFrame,
) -> tuple[float, float, list[list[float]]]:
    """Calculate map center and bounds from valid school latitude/longitude rows."""
    return (
        school_coordinates_df["latitude"].mean(),
        school_coordinates_df["longitude"].mean(),
        [
            [
                school_coordinates_df["latitude"].min(),
                school_coordinates_df["longitude"].min(),
            ],
            [
                school_coordinates_df["latitude"].max(),
                school_coordinates_df["longitude"].max(),
            ],
        ],
    )


def _format_popup_value(value) -> str:
    """Render a popup value, treating NaN / None / empty as 'N/A'."""
    if value is None or pd.isna(value):
        return "N/A"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    s = str(value).strip()
    return s if s else "N/A"


def _format_dq_flag(value) -> str:
    """Convert raw int DQ flag (1/0/None) to true/false string."""
    if value is None or pd.isna(value):
        return "N/A"
    try:
        return "true" if int(float(value)) == 1 else "false"
    except (TypeError, ValueError):
        return "N/A"


def _render_school_popup_html(
    row: dict,
    status: str,
    include_failure_reason: bool,
) -> str:
    """Render the HTML popup content for a school marker."""
    admin1 = _format_popup_value(row.get("admin1"))
    admin1_id = _format_popup_value(row.get("admin1_id_giga"))
    admin2 = _format_popup_value(row.get("admin2"))
    admin2_id = _format_popup_value(row.get("admin2_id_giga"))

    fields = [
        ("school_id_giga", _format_popup_value(row.get("school_id_giga"))),
        ("school_id_govt", _format_popup_value(row.get("school_id_govt"))),
        ("latitude", _format_popup_value(row.get("latitude"))),
        ("longitude", _format_popup_value(row.get("longitude"))),
        ("school_name", _format_popup_value(row.get("school_name"))),
        ("education_level", _format_popup_value(row.get("education_level"))),
        ("admin1", f"{admin1} ({admin1_id})"),
        ("admin2", f"{admin2} ({admin2_id})"),
        ("rurban", _format_popup_value(row.get("rurban_detected"))),
        ("uninhabited", _format_dq_flag(row.get(_UNINHABITED_COL))),
        ("duplicate_50_flag", _format_dq_flag(row.get(_DUP_FLAG_COL))),
        ("duplicate_50_group_id", _format_popup_value(row.get(_DUP_GROUP_COL))),
        ("duplicate_50_count", _format_popup_value(row.get(_DUP_COUNT_COL))),
        ("Status", status),
    ]
    if include_failure_reason:
        fields.append(("Reason", _format_popup_value(row.get("failure_reason"))))
    return _POPUP_TEMPLATE.render(fields=fields)


def _add_circle_marker(
    cluster: MarkerCluster,
    row: dict,
    color: str,
    popup_html: str,
) -> None:
    """Add one school coordinate marker to a Folium marker cluster."""
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=5,
        color=color,
        fill=True,
        fillColor=color,
        fillOpacity=0.7,
        popup=folium.Popup(popup_html, max_width=380),
    ).add_to(cluster)


def _get_rurality_filter_key(row: dict) -> str:
    """Return the rurality layer key for a school row."""
    rurban = str(row.get("rurban_detected", "")).strip().lower()
    if rurban == "urban":
        return "urban"
    if rurban == "rural":
        return "rural"
    return "unknown"


def _is_uninhabited(row: dict) -> bool:
    """Return whether a school row is flagged as in an uninhabited area."""
    try:
        return int(float(row.get(_UNINHABITED_COL, 0))) == 1
    except (TypeError, ValueError):
        return False


def _calculate_filter_counts(*dfs: pd.DataFrame) -> dict[str, int]:
    """Calculate filter layer counts using the same rules as marker placement."""
    counts = {"urban": 0, "rural": 0, "unknown": 0, "uninhabited": 0}
    for df in dfs:
        for row in df.to_dict("records"):
            counts[_get_rurality_filter_key(row)] += 1
            if _is_uninhabited(row):
                counts["uninhabited"] += 1
    return counts


def _add_school_markers_to_clusters(
    df: pd.DataFrame,
    base_color: str,
    status: str,
    main_cluster: MarkerCluster,
    filter_clusters: dict[str, MarkerCluster],
    include_failure_reason: bool,
) -> None:
    """Add school markers to the main status cluster and matching filter clusters."""
    for row in df.to_dict("records"):
        popup_html = _render_school_popup_html(
            row,
            status=status,
            include_failure_reason=include_failure_reason,
        )
        _add_circle_marker(main_cluster, row, base_color, popup_html)

        rurality_key = _get_rurality_filter_key(row)
        rurality_colors = {
            "urban": "#0d6efd",
            "rural": "#fd7e14",
            "unknown": "#6c757d",
        }
        _add_circle_marker(
            filter_clusters[rurality_key],
            row,
            rurality_colors[rurality_key],
            popup_html,
        )

        if _is_uninhabited(row):
            _add_circle_marker(
                filter_clusters["uninhabited"], row, "#6f42c1", popup_html
            )


def _add_non_empty_filter_clusters_to_map(
    m: folium.Map,
    filter_clusters: dict[str, MarkerCluster],
    counts: dict[str, int],
) -> None:
    """Attach populated filter clusters to the map with count labels."""
    cluster_labels = {
        "urban": "Urban",
        "rural": "Rural",
        "unknown": "Rurality Unknown",
        "uninhabited": "In Uninhabited Area",
    }
    for key, label in cluster_labels.items():
        if counts[key] > 0:
            filter_clusters[key].name = f"{label} ({counts[key]})"
            filter_clusters[key].add_to(m)


def generate_school_map_html(
    country_code: str,
    passed_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    context: OpExecutionContext,
) -> str:
    """Generate an optimized interactive HTML map with passed/failed schools and filter layers."""
    context.log.info(
        f"Map generation input: passed={len(passed_df)}, failed={len(failed_df)}"
    )

    passed_filtered = _filter_rows_with_valid_coordinates(passed_df)
    failed_filtered = _filter_rows_with_valid_coordinates(failed_df)

    if passed_filtered.empty and failed_filtered.empty:
        message = "No valid latitude/longitude data available for map generation"
        context.log.warning(message)
        raise ValueError(message)

    bounds_df = pd.concat([passed_filtered, failed_filtered], ignore_index=True)[
        ["latitude", "longitude"]
    ]
    center_lat, center_lon, bounds = _calculate_coordinate_bounds(bounds_df)

    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, control_scale=True)
    m.fit_bounds(bounds, padding=[50, 50])

    passed_cluster = MarkerCluster(
        name=f"Schools Passed ({len(passed_filtered)})", show=True
    )
    failed_cluster = MarkerCluster(
        name=f"Schools Failed ({len(failed_filtered)})", show=True
    )

    counts = _calculate_filter_counts(passed_filtered, failed_filtered)
    filter_clusters = {
        "urban": MarkerCluster(name=f"Urban ({counts['urban']})", show=False),
        "rural": MarkerCluster(name=f"Rural ({counts['rural']})", show=False),
        "unknown": MarkerCluster(
            name=f"Rurality Unknown ({counts['unknown']})", show=False
        ),
        "uninhabited": MarkerCluster(
            name=f"In Uninhabited Area ({counts['uninhabited']})", show=False
        ),
    }

    datasets = [
        (passed_filtered, PASSED_COLOR, "Passed", passed_cluster, False),
        (failed_filtered, FAILED_COLOR, "Failed", failed_cluster, True),
    ]

    for df, base_color, status, main_cluster, include_fail in datasets:
        _add_school_markers_to_clusters(
            df,
            base_color,
            status,
            main_cluster,
            filter_clusters,
            include_fail,
        )

    passed_cluster.add_to(m)
    failed_cluster.add_to(m)
    _add_non_empty_filter_clusters_to_map(m, filter_clusters, counts)

    folium.LayerControl(collapsed=False).add_to(m)
    context.log.info("Map generation complete successfully.")

    return m.get_root().render()
