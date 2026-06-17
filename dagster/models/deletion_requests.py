from datetime import datetime

from pydantic import UUID4
from sqlalchemy import VARCHAR, Boolean, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .base_database import BaseModel


class DeletionRequest(BaseModel):
    __tablename__ = "deletion_requests"

    requested_by_id: Mapped[UUID4] = mapped_column(VARCHAR(36), nullable=False)
    requested_by_email: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    requested_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    country: Mapped[str] = mapped_column(VARCHAR(3), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(), nullable=True)
    id_type: Mapped[str] = mapped_column(VARCHAR(20), nullable=True)
    school_count: Mapped[int] = mapped_column(Integer(), nullable=True)
    file_path: Mapped[str] = mapped_column(String(), nullable=True)
    raw_file_path: Mapped[str] = mapped_column(String(), nullable=True)
    is_delete_all: Mapped[bool] = mapped_column(Boolean(), nullable=True, default=False)
