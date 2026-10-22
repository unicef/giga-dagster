"""
ZabbixClient — reusable Zabbix API client compatible with V5 and V6.

All communication uses JSON-RPC 2.0 over HTTPS POST to {url}/api_jsonrpc.php.
V5/V6 compatibility is resolved once at login time via automatic version detection.

Version compatibility summary:
  Zabbix < 5.4  : user.login uses {"user": ..., "password": ...}
  Zabbix 5.4+   : user.login uses {"username": ..., "password": ...}
  Zabbix 6.0+   : API tokens can be used directly as the auth field value
  Zabbix 6.4+   : Authorization: Bearer header (body "auth" field removed in 7.2)
"""

import re
from typing import Any, Optional

import requests


class ZabbixClient:
    """
    Zabbix JSON-RPC 2.0 API client compatible with Zabbix V5 and V6.

    Usage:
        client = ZabbixClient(url="https://zabbix.example.com/zabbix",
                              username="Admin", password="zabbix")
        client.login()
        hosts = client.call("host.get", {"output": ["hostid", "host"], "limit": 10})
        client.logout()
    """

    def __init__(
        self,
        url: str,
        username: str = "",
        password: str = "",
        api_token: Optional[str] = None,
        ssl_verify: bool = True,
        http_auth: Optional[tuple[str, str]] = None,
    ):
        """
        Args:
            url:        Base URL of the Zabbix server (e.g. https://zabbix.example.com/zabbix).
                        Do not include /api_jsonrpc.php — it is appended automatically.
            username:   Zabbix username for user.login authentication.
            password:   Zabbix password for user.login authentication.
            api_token:  Optional V6+ API token. If provided, user.login is skipped
                        and this token is used directly as the auth value.
            ssl_verify: Whether to verify SSL certificates. Set False only for
                        development against self-signed certificates.
            http_auth:  Optional (user, password) for HTTP basic auth on a proxy
                        in front of the API (e.g. the Giga K8s Nginx proxy).
        """
        self.base_url = url.rstrip("/")
        self.api_url = f"{self.base_url}/api_jsonrpc.php"
        self.username = username
        self.password = password
        self.api_token = api_token
        self.ssl_verify = ssl_verify
        self.http_auth = http_auth

        self._auth_token: Optional[str] = None
        self._version: Optional[str] = None
        self._version_tuple: Optional[tuple[int, ...]] = None
        self._request_id: int = 0

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def get_version(self) -> str:
        """
        Return the Zabbix API version string (e.g. "6.0.14").
        Calls apiinfo.version — no authentication required.
        Result is cached after the first call.
        """
        if self._version is not None:
            return self._version

        result = self.call("apiinfo.version", {}, authenticated=False)
        self._version = str(result)
        # Regex so suffixes like "6.0.14rc1" don't break parsing
        self._version_tuple = tuple(
            int(x) for x in re.findall(r"\d+", self._version)[:3]
        )
        return self._version

    def login(self) -> str:
        """
        Authenticate against the Zabbix API.

        - If api_token was provided at construction, it is used directly
          (V6 API token flow — no user.login call is made).
        - Otherwise, calls user.login with version-appropriate param key:
            "username" for Zabbix 5.4+ / 6.x
            "user"     for Zabbix < 5.4

        Returns the auth token string (also stored in self._auth_token).
        """
        self.get_version()  # populates _version_tuple (decides body vs header auth)

        if self.api_token:
            self._auth_token = self.api_token
            return self._auth_token

        # "username" param introduced in 5.4, formally required from 6.0
        user_key = "username" if self._version_tuple >= (5, 4) else "user"

        result = self.call(
            "user.login",
            {user_key: self.username, "password": self.password},
            authenticated=False,
        )
        self._auth_token = str(result)
        return self._auth_token

    def logout(self) -> None:
        """
        Invalidate the current session by calling user.logout.
        Safe to call even if not logged in.
        For API token sessions, clears the local reference only
        (user.logout does not apply to API tokens).
        """
        if not self._auth_token:
            return
        if self.api_token:
            # API tokens are managed in Zabbix UI, not invalidated via user.logout
            self._auth_token = None
            return

        try:
            self.call("user.logout", {})
        finally:
            self._auth_token = None

    def call(
        self,
        method: str,
        params: dict[str, Any],
        authenticated: bool = True,
    ) -> Any:
        """
        Execute a single JSON-RPC 2.0 request.

        Args:
            method:        Zabbix API method name (e.g. "host.get").
            params:        Parameters dict for the method.
            authenticated: If True, sends the current auth token — in the body
                           "auth" field (< 6.4) or an Authorization: Bearer
                           header (6.4+). Pass False for apiinfo.version and
                           user.login, which must not send any auth.

        Returns:
            The "result" value from the JSON-RPC response.

        Raises:
            RuntimeError:                    On JSON-RPC error responses.
            requests.exceptions.HTTPError:   On non-2xx HTTP responses.
            requests.exceptions.Timeout:     If the request times out (30s).
        """
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_id(),
        }
        headers = {"Content-Type": "application/json"}

        if authenticated and self._auth_token:
            if self._version_tuple and self._version_tuple >= (6, 4):
                headers["Authorization"] = f"Bearer {self._auth_token}"
            else:
                payload["auth"] = self._auth_token

        response = requests.post(
            self.api_url,
            headers=headers,
            json=payload,
            verify=self.ssl_verify,
            auth=self.http_auth,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            error = data["error"]
            raise RuntimeError(
                f"Zabbix API error [{error.get('code')}]: "
                f"{error.get('message')} — {error.get('data', '')}"
            )

        return data["result"]
