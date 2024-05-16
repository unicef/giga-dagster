from datetime import datetime

from sqlalchemy import VARCHAR, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from .base_database import BaseModel


class MlabSchools(BaseModel):
    __tablename__ = "measurements"

    created: Mapped[str] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    school_id_govt: Mapped[int] = mapped_column(VARCHAR(36), nullable=False, index=True)
    source: Mapped[str] = mapped_column(VARCHAR(20), nullable=False, default="mlab")
    country_code: Mapped[str] = mapped_column(VARCHAR(2), nullable=False)


class GigaMeterSchools(BaseModel):
    __tablename__ = "gigameter_schools"

    created: Mapped[str] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    school_id_giga: Mapped[str] = mapped_column(VARCHAR(36), nullable=False)
    school_id_govt: Mapped[int] = mapped_column(nullable=False)
    source: Mapped[str] = mapped_column(nullable=False, default="daily_checkapp")


class RTSchools(BaseModel):
    __tablename__ = "RT_enabled_schools"

    school_id_giga: Mapped[str] = mapped_column(VARCHAR(36), nullable=False)
    school_id_govt: Mapped[str] = mapped_column(nullable=False)
    realtime_date: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    country_code: Mapped[str] = mapped_column(VARCHAR(2), nullable=False)
    country: Mapped[str] = mapped_column(VARCHAR(50))
