from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager
from typing import Any

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import Session, sessionmaker

from src.settings import settings

engine = create_engine(
    settings.INGESTION_DATABASE_URL,
    echo=not settings.IN_PRODUCTION,
    future=True,
)

session_maker = sessionmaker(
    bind=engine,
    autoflush=True,
    autocommit=False,
    expire_on_commit=False,
)


def get_db() -> Generator[Session, Any, Any]:
    session = session_maker()
    try:
        yield session
    except DatabaseError as err:
        logger.error(str(err))
        raise err
    finally:
        session.close()


@contextmanager
def get_db_context() -> AbstractContextManager[Session]:
    session = session_maker()
    try:
        yield session
    except DatabaseError as err:
        logger.error(str(err))
        raise err
    finally:
        session.close()
