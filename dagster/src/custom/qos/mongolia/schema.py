"""Schema definitions for Mongolia QoS outputs.

gold.qos.MNG PRD columns, excluding date/gigasync_id/signature (appended
downstream by adhoc__qos_transforms). This is process_mongolia_clean.py's shape
only - process_bandwidth_utilization.py's independent, less complete staging-only
transform (missing min/count, empty provider, no measurement_type) is not ported.
"""

from __future__ import annotations

MONGOLIA_RAW_CLEAN_SCHEMA: dict[str, str] = {
    "timestamp": "timestamp",
    "school_id_govt": "string",
    "school_id_giga": "string",
    "speed_download": "float",
    "speed_upload": "float",
    "inbound_traffic": "float",
    "outbound_traffic": "float",
    "in_perc": "float",
    "out_perc": "float",
    "device_id": "string",
    "country_id": "string",
    "measurement_id": "string",
}

MONGOLIA_GOLD_SCHEMA: dict[str, str] = {
    "timestamp": "timestamp",
    "country_id": "string",
    "school_id_giga": "string",
    "school_id_govt": "string",
    "provider": "string",
    "measurement_type": "string",
    "count": "integer",
    "speed_download_min": "float",
    "speed_download_mean": "float",
    "speed_download_max": "float",
    "speed_upload_min": "float",
    "speed_upload_mean": "float",
    "speed_upload_max": "float",
    "inbound_traffic_sum": "float",
    "outbound_traffic_sum": "float",
}
