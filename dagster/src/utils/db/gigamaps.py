from collections.abc import Generator
from contextlib import AbstractContextManager
from typing import Any

from sqlalchemy.orm import Session

from src.settings import settings

from .base import PostgresDatabaseProvider

# Lazy initialization of GigaMaps database provider
_gigamaps: PostgresDatabaseProvider | None = None


def _get_gigamaps_provider() -> PostgresDatabaseProvider:
    """Get the GigaMaps database provider, initializing it lazily."""
    global _gigamaps

    if _gigamaps is not None:
        return _gigamaps

    if not settings.GIGAMAPS_DB_CONNECTION_STRING:
        raise ValueError(
            "GIGAMAPS_DB_CONNECTION_STRING is not configured. "
            "Set it in your .env file to use GigaMaps database."
        )

    _gigamaps = PostgresDatabaseProvider(settings.GIGAMAPS_DB_CONNECTION_STRING)
    return _gigamaps


def get_db() -> Generator[Session, Any, Any]:
    """Get a GigaMaps database session generator."""
    return _get_gigamaps_provider().get_db()


def get_db_context() -> AbstractContextManager[Session]:
    """Get a GigaMaps database session context manager."""
    return _get_gigamaps_provider().get_db_context()
