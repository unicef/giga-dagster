from typing import Literal

from pydantic import BaseModel, conint, constr


class ApprovalRequestSchema(BaseModel):
    country: constr(min_length=3, max_length=3)
    dataset: str
    enabled: bool

    class Config:
        orm_mode = True


class CDFSelection(BaseModel):
    school_id_giga: str
    _change_type: Literal["insert", "update_preimage", "update_postimage", "delete"]
    _commit_version: conint(ge=0)
