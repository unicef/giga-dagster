"""Schema definition for Mawingu (KEN) QoS gold output.

gold.qos.KEN has 31 PRD columns total; date/gigasync_id/signature are appended
downstream by adhoc__qos_transforms, and a further 10 (ce_egress, ce_ingress,
pe_egress, pe_ingress, latency, latency_probe, speed_download, speed_download_probe,
speed_upload, speed_upload_probe) were meant to come from process_mawingu_clean.py,
which is commented out of the live crontab and was never active in prod - so this
schema intentionally covers only the 18 columns the live pipeline actually produces.
"""

from __future__ import annotations

MAWINGU_GOLD_SCHEMA: dict[str, str] = {
    "timestamp": "timestamp",
    "school_id_giga": "string",
    "school_id_govt": "string",
    "host_id": "string",
    "speed_download_mean": "float",
    "speed_download_max": "float",
    "speed_upload_mean": "float",
    "speed_upload_max": "float",
    # latency_min/mean/max and signal_mean/max are varchar in PRD (both prod and
    # stg agree - not a type mismatch), despite holding numeric aggregates.
    "latency_min": "string",
    "latency_mean": "string",
    "latency_max": "string",
    "signal_mean": "string",
    "signal_max": "string",
    "is_connected_all": "integer",
    "is_connected_true": "integer",
    "country_id": "string",
    "measurement_id": "string",
    "provider": "string",
}
