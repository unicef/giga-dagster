from src.settings import settings

from .base import PostgresDatabaseProvider

_primary = PostgresDatabaseProvider(settings.INGESTION_DATABASE_URL)

get_db = _primary.get_db

get_db_context = _primary.get_db_context
