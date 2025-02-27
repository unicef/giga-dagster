from src.settings import settings

from .base import TrinoDatabaseProvider

_trino = TrinoDatabaseProvider(settings.TRINO_CONNECTION_STRING)

get_db = _trino.get_db

get_db_context = _trino.get_db_context
