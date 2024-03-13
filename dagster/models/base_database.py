from collections.abc import Callable

from cuid2 import cuid_wrapper
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

cuid_generator: Callable[[], str] = cuid_wrapper()


class BaseModel(AsyncAttrs, DeclarativeBase):
    id: Mapped[str] = mapped_column(
        unique=True,
        primary_key=True,
        default=cuid_generator,
        index=True,
    )
