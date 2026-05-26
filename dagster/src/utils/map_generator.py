"""
Utility for generating interactive school maps using Folium.
"""

import folium
import pandas as pd
from folium.plugins import MarkerCluster

from dagster import OpExecutionContext


def generate_school_map_html(
    country_code: str,
    passed_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    context: OpExecutionContext,
) -> str:
    """Generate interactive HTML map with passed/failed schools."""
    all_lats: list[float] = []
    all_lons: list[float] = []

    if not passed_df.empty and "latitude" in passed_df.columns:
        all_lats.extend(passed_df["latitude"].dropna().tolist())
        all_lons.extend(passed_df["longitude"].dropna().tolist())

    if not failed_df.empty and "latitude" in failed_df.columns:
        all_lats.extend(failed_df["latitude"].dropna().tolist())
        all_lons.extend(failed_df["longitude"].dropna().tolist())

    if not all_lats:
        context.log.warning("No location data available for map generation")
        center_lat, center_lon = 0.0, 0.0
        bounds = None
    else:
        center_lat = sum(all_lats) / len(all_lats)
        center_lon = sum(all_lons) / len(all_lons)
        bounds = [
            [min(all_lats), min(all_lons)],
            [max(all_lats), max(all_lons)],
        ]

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        control_scale=True,
    )

    if bounds:
        m.fit_bounds(bounds, padding=[50, 50])

    passed_cluster = MarkerCluster(name="Schools Passed", overlay=True, control=True)
    failed_cluster = MarkerCluster(name="Schools Rejected", overlay=True, control=True)

    passed_count = 0
    if (
        not passed_df.empty
        and "latitude" in passed_df.columns
        and "longitude" in passed_df.columns
    ):
        valid_passed = passed_df.dropna(subset=["latitude", "longitude"])
        for row in valid_passed.itertuples():
            folium.CircleMarker(
                location=[row.latitude, row.longitude],
                radius=5,
                color="#28a745",
                fill=True,
                fillColor="#28a745",
                fillOpacity=0.7,
                popup=(
                    f"<b>School:</b> {getattr(row, 'school_name', 'N/A')}<br>"
                    f"<b>Status:</b> Passed"
                ),
            ).add_to(passed_cluster)
            passed_count += 1

    failed_count = 0
    if (
        not failed_df.empty
        and "latitude" in failed_df.columns
        and "longitude" in failed_df.columns
    ):
        valid_failed = failed_df.dropna(subset=["latitude", "longitude"])
        for row in valid_failed.itertuples():
            folium.CircleMarker(
                location=[row.latitude, row.longitude],
                radius=5,
                color="#dc3545",
                fill=True,
                fillColor="#dc3545",
                fillOpacity=0.7,
                popup=(
                    f"<b>School:</b> {getattr(row, 'school_name', 'N/A')}<br>"
                    f"<b>Status:</b> Failed<br>"
                    f"<b>Reason:</b> {getattr(row, 'failure_reason', 'N/A')}"
                ),
            ).add_to(failed_cluster)
            failed_count += 1

    passed_cluster.add_to(m)
    failed_cluster.add_to(m)
    folium.LayerControl().add_to(m)

    context.log.info(
        f"Added {passed_count} passed schools and {failed_count} failed schools to map"
    )

    return m.get_root().render()
