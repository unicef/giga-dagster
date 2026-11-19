"""Schema definition for Brazil QoS gold output."""

from __future__ import annotations

BRA_QOS_SCHEMA: dict[str, str] = {
    "timestamp": "timestamp",
    "country_id": "string",
    "school_id_govt": "string",
    "school_id_giga": "string",
    "speed_download": "float",
    "speed_upload": "float",
    "roundtrip_time": "float",
    "jitter_download": "float",
    "jitter_upload": "float",
    "rtt_packet_loss_pct": "float",
    "latency": "float",
    "provider": "string",
    "ip_family": "integer",
    "report_id": "string",
    "agent_id": "string",
}
