from pydantic import BaseModel, constr


class ApprovalRequestSchema(BaseModel):
    country: constr(min_length=3, max_length=3)
    dataset: str
    enabled: bool

    class Config:
        orm_mode = True
