"""Isizwe (South Africa)-specific QoS constants."""

from __future__ import annotations

COUNTRY_CODE = "ZAF"

# Zabbix history item key -> our column name
ITEM_KEY_MAP: dict[str, str] = {
    "icmppingsec": "latency",
    "icmppingloss": "ping_loss",
    "icmpping": "ping_status",
    "net.if.in[ifHCInOctets.2]": "speed_download",
    "net.if.out[ifHCOutOctets.2]": "speed_upload",
}
