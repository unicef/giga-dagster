"""Mawingu (Kenya)-specific QoS constants."""

from __future__ import annotations

COUNTRY_CODE = "KEN"

# host_id -> school_id_giga/school_id_govt reference data has no Spark table
# equivalent (unlike VCT's custom_dataset.device_matched or Isizwe's Zabbix host
# tags) - it must be uploaded to ADLS ahead of time and kept in sync manually,
# same as mawingu_schools.csv is today on the VM.
REFERENCE_SCHOOLS_FILEPATH = "reference/qos/KEN/mawingu_schools.csv"

# Zabbix history item key -> our column name
ITEM_KEY_MAP: dict[str, str] = {
    "icmppingsec": "latency",
    "icmppingloss": "ping_loss",
    "icmpping": "ping_status",
    "net.if.in[ifInOctets.2]": "speed_upload",
    "net.if.in[ifInOctets.4]": "speed_upload",
    "net.if.out[ifOutOctets.2]": "speed_download",
    "net.if.out[ifOutOctets.4]": "speed_download",
}
