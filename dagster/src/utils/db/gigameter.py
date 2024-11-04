from src.settings import settings

from .base import PostgresDatabaseProvider

_gigameter = PostgresDatabaseProvider(settings.GIGAMETER_DB_CONNECTION_STRING)

get_db = _gigameter.get_db

get_db_context = _gigameter.get_db_context
