"""Minimal Zabbix JSON-RPC 2.0 client (Zabbix 5.0-7.x) for the URY retest.

< 5.4 : user.login takes "user"; 5.4+ takes "username"
< 7.2 : token goes in the body "auth" field; 7.2+ needs an Authorization: Bearer header
"""

import re
from typing import Any

import requests


class ZabbixClient:
    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        http_auth: tuple[str, str] | None = None,
    ):
        """url: Zabbix base URL (api_jsonrpc.php is appended).
        http_auth: optional (user, password) for basic auth on the Giga proxy."""
        self.api_url = f"{url.rstrip('/')}/api_jsonrpc.php"
        self.username = username
        self.password = password
        self.http_auth = http_auth
        self._auth_token: str | None = None
        self._version: str | None = None
        self._version_tuple: tuple[int, ...] | None = None
        self._request_id = 0

    def get_version(self) -> str:
        """apiinfo.version (no auth). Cached."""
        if self._version is None:
            self._version = str(self.call("apiinfo.version", {}, authenticated=False))
            self._version_tuple = tuple(
                int(x) for x in re.findall(r"\d+", self._version)[:3]
            )
        return self._version

    def login(self) -> str:
        self.get_version()
        user_key = "username" if self._version_tuple >= (5, 4) else "user"
        self._auth_token = str(
            self.call(
                "user.login",
                {user_key: self.username, "password": self.password},
                authenticated=False,
            )
        )
        return self._auth_token

    def logout(self) -> None:
        if self._auth_token:
            try:
                self.call("user.logout", {})
            finally:
                self._auth_token = None

    def call(
        self, method: str, params: dict[str, Any], authenticated: bool = True
    ) -> Any:
        """One JSON-RPC request. Raises RuntimeError on API errors, HTTPError on non-2xx."""
        self._request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._request_id,
        }
        headers = {"Content-Type": "application/json"}

        if authenticated and self._auth_token:
            # Body auth up to 7.0 keeps the Authorization header free for proxy basic auth.
            if self._version_tuple and self._version_tuple >= (7, 2):
                if self.http_auth:
                    raise RuntimeError(
                        "Zabbix 7.2+ needs the Authorization header, which clashes with proxy basic auth"
                    )
                headers["Authorization"] = f"Bearer {self._auth_token}"
            else:
                payload["auth"] = self._auth_token

        response = requests.post(
            self.api_url, headers=headers, json=payload, auth=self.http_auth, timeout=30
        )
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            e = data["error"]
            raise RuntimeError(
                f"Zabbix API error [{e.get('code')}]: {e.get('message')} - {e.get('data', '')}"
            )
        return data["result"]
