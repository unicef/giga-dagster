from src.settings import settings

from .base import PostgresDatabaseProvider

_gigamaps = PostgresDatabaseProvider(settings.GIGAMAPS_DB_CONNECTION_STRING)

get_db = _gigamaps.get_db

get_db_context = _gigamaps.get_db_context
