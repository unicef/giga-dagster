from sqlalchemy import VARCHAR, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base_database import BaseModel


class ApprovalRequest(BaseModel):
    __tablename__ = "approval_requests"
    __table_args__ = (
        UniqueConstraint("country", "dataset", name="uq_country_dataset"),
    )

    country: Mapped[str] = mapped_column(VARCHAR(3), nullable=False)
    dataset: Mapped[str] = mapped_column(nullable=False)
    enabled: Mapped[bool] = mapped_column(default=False)
