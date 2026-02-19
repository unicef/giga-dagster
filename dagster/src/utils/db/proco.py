from collections.abc import Generator
from contextlib import AbstractContextManager
from typing import Any

from sqlalchemy.orm import Session

from src.settings import settings

from .base import PostgresDatabaseProvider

# Lazy initialization of Proco database provider
_proco: PostgresDatabaseProvider | None = None


def _get_proco_provider() -> PostgresDatabaseProvider:
    """Get the Proco database provider, initializing it lazily."""
    global _proco

    if _proco is not None:
        return _proco

    if not settings.PROCO_DB_CONNECTION_STRING:
        raise ValueError(
            "PROCO_DB_CONNECTION_STRING is not configured. "
            "Set it in your .env file to use Proco database."
        )

    _proco = PostgresDatabaseProvider(settings.PROCO_DB_CONNECTION_STRING)
    return _proco


def get_db() -> Generator[Session, Any, Any]:
    """Get a Proco database session generator."""
    return _get_proco_provider().get_db()


def get_db_context() -> AbstractContextManager[Session]:
    """Get a Proco database session context manager."""
    return _get_proco_provider().get_db_context()
