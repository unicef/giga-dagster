from datetime import datetime

from pydantic import UUID4, BaseModel, Field, constr


class ApprovalRequestAuditLogSchema(BaseModel):
    approval_request_id: str
    approved_by_id: UUID4
    approved_by_email: str
    approved_date: datetime

    class Config:
        orm_mode = True


class ApprovalRequestSchema(BaseModel):
    country: constr(min_length=3, max_length=3)
    dataset: str
    enabled: bool
    is_merge_processing: bool
    audit_logs: list[ApprovalRequestAuditLogSchema] = Field(default_factory=list)

    class Config:
        orm_mode = True
