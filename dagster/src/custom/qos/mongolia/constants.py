"""Mongolia-specific QoS constants."""

from __future__ import annotations

COUNTRY_CODE = "MNG"

# device_id -> fetch_url + device metadata reference data has no Spark table
# equivalent - it must be uploaded to ADLS ahead of time and kept in sync
# manually, same as whitelisted_devices.csv is today on the VM.
REFERENCE_DEVICES_FILEPATH = "reference/qos/MNG/whitelisted_devices.csv"

RAW_JSON_PREFIX = f"raw/qos/{COUNTRY_CODE}"

CONVERSION_FACTORS: dict[str, float] = {
    "Tbps": 1000000,
    "Gbps": 1000,
    "Mbps": 1,
    "kbps": 0.001,
    "bps": 0.000001,
}
