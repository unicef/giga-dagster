from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager
from typing import Any

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import Session, sessionmaker

from src.settings import settings


class PostgresDatabaseProvider:
    def __init__(self, url: str):
        if url == "":
            raise ValueError("Database URL is empty")

        self.engine = create_engine(
            url,
            echo=not settings.IN_PRODUCTION,
            future=True,
        )
        self.session_maker = sessionmaker(
            bind=self.engine,
            autoflush=True,
            autocommit=False,
            expire_on_commit=False,
        )

    def get_db(self) -> Generator[Session, Any, Any]:
        session = self.session_maker()
        try:
            yield session
        except DatabaseError as err:
            logger.error(str(err))
            raise err
        finally:
            session.close()

    @contextmanager
    def get_db_context(self) -> AbstractContextManager[Session]:
        session = self.session_maker()
        try:
            yield session
        except DatabaseError as err:
            logger.error(str(err))
            raise err
        finally:
            session.close()


class TrinoDatabaseProvider:
    def __init__(self, url: str):
        if url == "":
            raise ValueError("Database URL is empty")

        self.engine = create_engine(
            url,
            echo=not settings.IN_PRODUCTION,
            future=True,
            connect_args={"http_scheme": "https"},
        )
        self.session_maker = sessionmaker(
            bind=self.engine,
            autoflush=True,
            autocommit=False,
            expire_on_commit=False,
        )

    def get_db(self) -> Generator[Session, Any, Any]:
        session = self.session_maker()
        try:
            yield session
        except DatabaseError as err:
            logger.error(str(err))
            raise err
        finally:
            session.close()

    @contextmanager
    def get_db_context(self) -> AbstractContextManager[Session]:
        session = self.session_maker()
        try:
            yield session
        except DatabaseError as err:
            logger.error(str(err))
            raise err
        finally:
            session.close()
