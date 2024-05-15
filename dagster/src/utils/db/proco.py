from src.settings import settings

from .base import PostgresDatabaseProvider

_proco = PostgresDatabaseProvider(settings.PROCO_DB_CONNECTION_STRING)

get_db = _proco.get_db

get_db_context = _proco.get_db_context
