from datetime import datetime
from typing import Annotated

from pydantic import UUID4, BaseModel, ConfigDict, Field, StringConstraints


class ApprovalRequestAuditLogSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    approval_request_id: str
    approved_by_id: UUID4
    approved_by_email: str
    approved_date: datetime


class ApprovalRequestSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    country: Annotated[str, StringConstraints(min_length=3, max_length=3)]
    dataset: str
    enabled: bool
    is_merge_processing: bool
    audit_logs: list[ApprovalRequestAuditLogSchema] = Field(default_factory=list)
