from collections.abc import Generator
from contextlib import AbstractContextManager
from typing import Any

from sqlalchemy.orm import Session

from src.settings import settings

from .base import PostgresDatabaseProvider

# Lazy initialization of MLab database provider
_mlab: PostgresDatabaseProvider | None = None


def _get_mlab_provider() -> PostgresDatabaseProvider:
    """Get the MLab database provider, initializing it lazily."""
    global _mlab

    if _mlab is not None:
        return _mlab

    if not settings.MLAB_DB_CONNECTION_STRING:
        raise ValueError(
            "MLAB_DB_CONNECTION_STRING is not configured. "
            "Set it in your .env file to use MLab database."
        )

    _mlab = PostgresDatabaseProvider(settings.MLAB_DB_CONNECTION_STRING)
    return _mlab


def get_db() -> Generator[Session, Any, Any]:
    """Get a MLab database session generator."""
    return _get_mlab_provider().get_db()


def get_db_context() -> AbstractContextManager[Session]:
    """Get a MLab database session context manager."""
    return _get_mlab_provider().get_db_context()
