from collections.abc import Generator
from contextlib import AbstractContextManager
from typing import Any

from sqlalchemy.orm import Session

from src.settings import settings

from .base import TrinoDatabaseProvider

# Lazy initialization of Trino database providers
_trino: TrinoDatabaseProvider | None = None
_analytics_trino: TrinoDatabaseProvider | None = None


def _get_trino_provider() -> TrinoDatabaseProvider:
    """Get the Trino database provider, initializing it lazily."""
    global _trino

    if _trino is not None:
        return _trino

    if not settings.TRINO_CONNECTION_STRING:
        raise ValueError(
            "TRINO_CONNECTION_STRING is not configured. "
            "Set it in your .env file to use Trino."
        )

    _trino = TrinoDatabaseProvider(settings.TRINO_CONNECTION_STRING)
    return _trino


def _get_analytics_trino_provider() -> TrinoDatabaseProvider:
    """Get the analytics Trino database provider, initializing it lazily."""
    global _analytics_trino

    if _analytics_trino is not None:
        return _analytics_trino

    if not settings.ANALYTICS_TRINO_CONNECTION_STRING:
        raise ValueError(
            "ANALYTICS_TRINO_CONNECTION_STRING is not configured. "
            "Set it in your .env file to use analytics Trino."
        )

    _analytics_trino = TrinoDatabaseProvider(settings.ANALYTICS_TRINO_CONNECTION_STRING)
    return _analytics_trino


def get_db() -> Generator[Session, Any, Any]:
    """Get a Trino database session generator."""
    return _get_trino_provider().get_db()


def get_db_context() -> AbstractContextManager[Session]:
    """Get a Trino database session context manager."""
    return _get_trino_provider().get_db_context()


def get_analytics_db() -> Generator[Session, Any, Any]:
    """Get an analytics Trino database session generator."""
    return _get_analytics_trino_provider().get_db()


def get_analytics_db_context() -> AbstractContextManager[Session]:
    """Get an analytics Trino database session context manager."""
    return _get_analytics_trino_provider().get_db_context()
