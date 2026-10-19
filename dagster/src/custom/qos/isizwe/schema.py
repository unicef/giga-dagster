"""Schema definition for Isizwe (ZAF) QoS gold output."""

from __future__ import annotations

ISIZWE_GOLD_SCHEMA: dict[str, str] = {
    "timestamp": "timestamp",
    "school_id_giga": "string",
    "school_id_govt": "string",
    "host_id": "string",
    "speed_download_min": "float",
    "speed_download_mean": "float",
    "speed_download_max": "float",
    "speed_upload_min": "float",
    "speed_upload_mean": "float",
    "speed_upload_max": "float",
    "is_connected_all": "integer",
    "is_connected_true": "integer",
    "country_id": "string",
    "measurement_id": "string",
    "provider": "string",
}
