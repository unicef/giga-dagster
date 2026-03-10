from collections.abc import Generator
from contextlib import AbstractContextManager
from typing import Any

from sqlalchemy.orm import Session

from src.settings import settings

from .base import PostgresDatabaseProvider

# Lazy initialization of GigaMeter database provider
_gigameter: PostgresDatabaseProvider | None = None


def _get_gigameter_provider() -> PostgresDatabaseProvider:
    """Get the GigaMeter database provider, initializing it lazily."""
    global _gigameter

    if _gigameter is not None:
        return _gigameter

    if not settings.GIGAMETER_DB_CONNECTION_STRING:
        raise ValueError(
            "GIGAMETER_DB_CONNECTION_STRING is not configured. "
            "Set it in your .env file to use GigaMeter database."
        )

    _gigameter = PostgresDatabaseProvider(settings.GIGAMETER_DB_CONNECTION_STRING)
    return _gigameter


def get_db() -> Generator[Session, Any, Any]:
    """Get a GigaMeter database session generator."""
    return _get_gigameter_provider().get_db()


def get_db_context() -> AbstractContextManager[Session]:
    """Get a GigaMeter database session context manager."""
    return _get_gigameter_provider().get_db_context()
