from collections.abc import Generator
from contextlib import AbstractContextManager
from typing import Any

from sqlalchemy.orm import Session

from src.settings import settings

from .base import TrinoDatabaseProvider

# Lazy initialization of Trino database provider
_trino: TrinoDatabaseProvider | None = None


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


def get_db() -> Generator[Session, Any, Any]:
    """Get a Trino database session generator."""
    return _get_trino_provider().get_db()


def get_db_context() -> AbstractContextManager[Session]:
    """Get a Trino database session context manager."""
    return _get_trino_provider().get_db_context()
