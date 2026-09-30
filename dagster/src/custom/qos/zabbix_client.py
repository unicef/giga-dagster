"""Shared Zabbix JSON-RPC client — reusable across country integrations (Isizwe, Mawingu)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Optional

import requests


def make_api_call(
    api_url: str, auth_token: str, method: str, params: Optional[dict] = None
) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params if params else {},
        "id": 1,
        "auth": auth_token,
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(api_url, headers=headers, json=payload, timeout=60)
    response_data = response.json()

    if "result" in response_data:
        return response_data["result"]
    raise Exception(
        f"Zabbix API call failed: {response_data.get('error', 'Unknown error')}"
    )


def timestamp_to_unix(timestamp: str) -> int:
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp())
