"""
Utility for generating interactive school maps using Folium.

Each school gets exactly one marker (no per-filter duplication). Filtering is
done client-side via a hand-written grouped-checkbox Leaflet control: four
independent filter groups (status / rurality / habitability / boundary),
AND across groups, OR within a group - a marker shows only if it matches at
least one checked value in every group.
"""

import json

import folium
import pandas as pd
from jinja2 import BaseLoader, Environment

from dagster import OpExecutionContext

PASSED_COLOR = "#28a745"
FAILED_COLOR = "#dc3545"
OUTSIDE_BOUNDARY_COLOR = "#343a40"

_UNINHABITED_COL = "dq_is_in_uninhabited_area"
_DUP_FLAG_COL = "dq_duplicate_group_flag_50m"
_DUP_COUNT_COL = "dq_duplicate_group_count_50m"
_DUP_GROUP_COL = "dq_duplicate_group_id_50m"
_OUTSIDE_BOUNDARY_COL = "dq_is_not_within_country"

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

# group key -> ordered list of (tag_value, checkbox_label)
_FILTER_GROUPS = {
    "status": [("passed", "Passed"), ("failed", "Failed")],
    "rurality": [
        ("rural", "Rural"),
        ("urban", "Urban"),
        ("unknown", "Rurality unknown"),
    ],
    "habitability": [("inhabited", "Inhabited"), ("uninhabited", "Uninhabited")],
    "boundary": [("inside", "Inside boundary"), ("outside", "Outside boundary")],
}
_FILTER_GROUP_TITLES = {
    "status": "Data Quality Status",
    "rurality": "Rurality",
    "habitability": "Habitability",
    "boundary": "Country Boundary",
}


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
        ("outside_country", _format_dq_flag(row.get(_OUTSIDE_BOUNDARY_COL))),
        ("duplicate_50_flag", _format_dq_flag(row.get(_DUP_FLAG_COL))),
        ("duplicate_50_group_id", _format_popup_value(row.get(_DUP_GROUP_COL))),
        ("duplicate_50_count", _format_popup_value(row.get(_DUP_COUNT_COL))),
        ("Status", status),
    ]
    if include_failure_reason:
        fields.append(("Reason", _format_popup_value(row.get("failure_reason"))))
    return _POPUP_TEMPLATE.render(fields=fields)


def _add_circle_marker(
    layer: folium.FeatureGroup,
    row: dict,
    color: str,
    popup_html: str,
    tags: list[str],
) -> None:
    """Add one school coordinate marker to the shared feature group."""
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=1,
        color=color,
        fill=True,
        fillColor=color,
        fillOpacity=0.7,
        popup=folium.Popup(popup_html, max_width=380),
        tags=tags,
    ).add_to(layer)


def _get_rurality_filter_key(row: dict) -> str:
    """Return the rurality tag for a school row."""
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


def _is_outside_boundary(row: dict) -> bool:
    """Return whether a school row is flagged as outside the country boundary."""
    try:
        return int(float(row.get(_OUTSIDE_BOUNDARY_COL, 0))) == 1
    except (TypeError, ValueError):
        return False


def _add_school_markers(
    df: pd.DataFrame,
    base_color: str,
    status_tag: str,
    status_label: str,
    main_layer: folium.FeatureGroup,
    include_failure_reason: bool,
) -> None:
    """Add one tagged school marker per row to the shared layer."""
    for row in df.to_dict("records"):
        popup_html = _render_school_popup_html(
            row,
            status=status_label,
            include_failure_reason=include_failure_reason,
        )
        outside_boundary = _is_outside_boundary(row)
        tags = [
            status_tag,
            _get_rurality_filter_key(row),
            "uninhabited" if _is_uninhabited(row) else "inhabited",
            "outside" if outside_boundary else "inside",
        ]
        color = OUTSIDE_BOUNDARY_COLOR if outside_boundary else base_color
        _add_circle_marker(main_layer, row, color, popup_html, tags)


def _calculate_filter_counts(
    passed_df: pd.DataFrame, failed_df: pd.DataFrame
) -> dict[str, int]:
    """Calculate per-tag-value counts for the grouped filter control's checkbox labels."""
    counts = {"passed": len(passed_df), "failed": len(failed_df)}
    for df in (passed_df, failed_df):
        for row in df.to_dict("records"):
            rurality_key = _get_rurality_filter_key(row)
            counts[rurality_key] = counts.get(rurality_key, 0) + 1
            habit_key = "uninhabited" if _is_uninhabited(row) else "inhabited"
            counts[habit_key] = counts.get(habit_key, 0) + 1
            boundary_key = "outside" if _is_outside_boundary(row) else "inside"
            counts[boundary_key] = counts.get(boundary_key, 0) + 1
    return counts


def _build_grouped_filter_control_js(layer_var: str, counts: dict[str, int]) -> str:
    """Build the <script> body for the custom grouped-checkbox filter control."""
    groups_values = {
        group: [value for value, _label in options]
        for group, options in _FILTER_GROUPS.items()
    }

    sections_html = []
    for group, options in _FILTER_GROUPS.items():
        rows = "".join(
            f'<label class="gfc-row"><input type="checkbox" class="gfc" '
            f'data-group="{group}" value="{value}" checked> {label} '
            f"({counts.get(value, 0):,})</label>"
            for value, label in options
        )
        sections_html.append(
            f'<div class="gfc-group"><div class="gfc-title">'
            f"{_FILTER_GROUP_TITLES[group]}</div>{rows}</div>"
        )
    panel_html = "".join(sections_html).replace("`", "'")

    return f"""
    var GFC_GROUPS = {json.dumps(groups_values)};
    var GFC_ALL_MARKERS = [];
    {layer_var}.eachLayer(function(l) {{ GFC_ALL_MARKERS.push(l); }});

    function gfcApplyFilters() {{
        var active = {{}};
        Object.keys(GFC_GROUPS).forEach(function(g) {{ active[g] = new Set(); }});
        document.querySelectorAll("input.gfc:checked").forEach(function(cb) {{
            active[cb.getAttribute("data-group")].add(cb.value);
        }});
        GFC_ALL_MARKERS.forEach(function(marker) {{
            var visible = Object.keys(GFC_GROUPS).every(function(g) {{
                return marker.options.tags.some(function(t) {{ return active[g].has(t); }});
            }});
            var has = {layer_var}.hasLayer(marker);
            if (visible && !has) {{ {layer_var}.addLayer(marker); }}
            if (!visible && has) {{ {layer_var}.removeLayer(marker); }}
        }});
    }}

    var GroupedFilterControl = L.Control.extend({{
        options: {{position: "topright"}},
        onAdd: function(map) {{
            var container = L.DomUtil.create("div", "gfc-panel leaflet-bar");
            container.innerHTML = `{panel_html}`;
            L.DomEvent.disableClickPropagation(container);
            container.querySelectorAll("input.gfc").forEach(function(cb) {{
                cb.addEventListener("change", gfcApplyFilters);
            }});
            return container;
        }}
    }});
    """


def generate_school_map_html(
    country_code: str,
    passed_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    context: OpExecutionContext,
) -> str:
    """Generate an optimized interactive HTML map with grouped checkbox filters."""
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

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        control_scale=True,
        prefer_canvas=True,
    )
    m.fit_bounds(bounds, padding=[50, 50])

    main_layer = folium.FeatureGroup(name="Schools", show=True)

    _add_school_markers(
        passed_filtered, PASSED_COLOR, "passed", "Passed", main_layer, False
    )
    _add_school_markers(
        failed_filtered, FAILED_COLOR, "failed", "Failed", main_layer, True
    )
    main_layer.add_to(m)

    counts = _calculate_filter_counts(passed_filtered, failed_filtered)
    js = _build_grouped_filter_control_js(main_layer.get_name(), counts)
    extra_markup = (
        "<style>.gfc-panel{background:white;padding:8px 10px;"
        "border-radius:4px;box-shadow:0 1px 5px rgba(0,0,0,0.4);"
        "font-size:13px;max-height:420px;overflow:auto;}"
        ".gfc-title{font-weight:bold;margin-top:6px;}"
        ".gfc-row{display:block;white-space:nowrap;margin:2px 0;}</style>"
        f"<script>{js}\n"
        f"new GroupedFilterControl().addTo({m.get_name()});</script>"
    )

    context.log.info("Map generation complete successfully.")

    # Render first, then append our control's markup/JS as plain text so it
    # always executes after Folium's own marker-creation script tags, which
    # define the layer variable our control depends on. Folium places its
    # </body> tag *before* the <script> block that actually creates the
    # markers, so replacing </body> still runs too early - appending at the
    # absolute end (even after </html>) is the only ordering guarantee,
    # and trailing <script> tags still execute fine in browsers.
    html = m.get_root().render()
    return html + extra_markup
