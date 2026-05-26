"""
Utility for generating interactive school maps using Folium.
"""

import folium
import pandas as pd
from folium.plugins import MarkerCluster

from dagster import OpExecutionContext


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


def _add_school_markers(
    df: pd.DataFrame,
    cluster: MarkerCluster,
    color: str,
    status: str,
    include_failure_reason: bool = False,
) -> int:
    marker_count = 0
    for row in df.itertuples():
        popup = (
            f"<b>School:</b> {getattr(row, 'school_name', 'N/A')}<br>"
            f"<b>Status:</b> {status}"
        )
        if include_failure_reason:
            popup += f"<br><b>Reason:</b> {getattr(row, 'failure_reason', 'N/A')}"

        folium.CircleMarker(
            location=[row.latitude, row.longitude],
            radius=5,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            popup=popup,
        ).add_to(cluster)
        marker_count += 1

    return marker_count


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

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        control_scale=True,
    )

    if bounds:
        m.fit_bounds(bounds, padding=[50, 50])

    passed_cluster = MarkerCluster(name="Schools Passed", overlay=True, control=True)
    failed_cluster = MarkerCluster(name="Schools Rejected", overlay=True, control=True)

    passed_count = _add_school_markers(
        passed_df,
        passed_cluster,
        color="#28a745",
        status="Passed",
    )
    failed_count = _add_school_markers(
        failed_df,
        failed_cluster,
        color="#dc3545",
        status="Failed",
        include_failure_reason=True,
    )

    passed_cluster.add_to(m)
    failed_cluster.add_to(m)
    folium.LayerControl().add_to(m)

    context.log.info(
        f"Added {passed_count} passed schools and {failed_count} failed schools to map"
    )

    return m.get_root().render()
