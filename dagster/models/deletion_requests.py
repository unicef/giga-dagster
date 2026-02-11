from datetime import datetime

from pydantic import UUID4
from sqlalchemy import VARCHAR, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import BaseModel


class DeletionRequest(BaseModel):
    __tablename__ = "deletion_requests"

    requested_by_id: Mapped[UUID4] = mapped_column(VARCHAR(36), nullable=False)
    requested_by_email: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    requested_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    country: Mapped[str] = mapped_column(VARCHAR(3), nullable=False)
