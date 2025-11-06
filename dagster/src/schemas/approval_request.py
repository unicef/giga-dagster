from datetime import datetime
from typing import Annotated

from pydantic import UUID4, BaseModel, Field, StringConstraints


class ApprovalRequestAuditLogSchema(BaseModel):
    approval_request_id: str
    approved_by_id: UUID4
    approved_by_email: str
    approved_date: datetime

    model_config = {"from_attributes": True}


class ApprovalRequestSchema(BaseModel):
    country: Annotated[str, StringConstraints(min_length=3, max_length=3)]
    dataset: str
    enabled: bool
    is_merge_processing: bool
    audit_logs: list[ApprovalRequestAuditLogSchema] = Field(default_factory=list)

    model_config = {"from_attributes": True}
