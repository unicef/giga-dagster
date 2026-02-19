from collections.abc import Generator
from contextlib import AbstractContextManager
from typing import Any

from sqlalchemy.orm import Session

from src.settings import settings

from .base import PostgresDatabaseProvider

# Lazy initialization of primary (ingestion) database provider
_primary: PostgresDatabaseProvider | None = None


def _get_primary_provider() -> PostgresDatabaseProvider:
    """Get the primary database provider, initializing it lazily."""
    global _primary

    if _primary is not None:
        return _primary

    if not settings.INGESTION_DATABASE_URL:
        raise ValueError(
            "INGESTION_DATABASE_URL is not configured. "
            "Check your database settings in .env file."
        )

    _primary = PostgresDatabaseProvider(settings.INGESTION_DATABASE_URL)
    return _primary


def get_db() -> Generator[Session, Any, Any]:
    """Get a primary database session generator."""
    return _get_primary_provider().get_db()


def get_db_context() -> AbstractContextManager[Session]:
    """Get a primary database session context manager."""
    return _get_primary_provider().get_db_context()
