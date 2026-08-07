"""Shared Meraki API client and retry wrapper — reusable across country integrations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

import meraki
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

T = TypeVar("T")


def _retryable(exc: BaseException) -> bool:
    if not isinstance(exc, meraki.APIError):
        return False
    return exc.status == 429 or (exc.status is not None and exc.status >= 500)


@retry(
    wait=wait_exponential_jitter(initial=1, max=20),
    stop=stop_after_attempt(5),
    retry=retry_if_exception(_retryable),
    reraise=True,
)
def call(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    return fn(*args, **kwargs)


def create_dashboard(api_key: str) -> meraki.DashboardAPI:
    return meraki.DashboardAPI(api_key, suppress_logging=True)
