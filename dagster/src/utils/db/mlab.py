from src.settings import settings

from .base import PostgresDatabaseProvider

_mlab = PostgresDatabaseProvider(settings.MLAB_DB_CONNECTION_STRING)

get_db = _mlab.get_db

get_db_context = _mlab.get_db_context
