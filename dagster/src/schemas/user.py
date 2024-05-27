from typing import Literal

from pydantic import UUID4, BaseModel, EmailStr


class GraphGroup(BaseModel):
    id: UUID4
    description: str | None
    display_name: str

    class Config:
        orm_mode = True


class GraphIdentity(BaseModel):
    issuer: str
    issuer_assigned_id: str | None
    sign_in_type: str

    class Config:
        orm_mode = True


class GraphUser(BaseModel):
    id: UUID4
    account_enabled: bool | None
    given_name: str | None
    surname: str | None
    mail: EmailStr | None
    display_name: str | None
    user_principal_name: EmailStr
    external_user_state: Literal["Accepted", "PendingAcceptance"] | None
    member_of: list[GraphGroup] | None
    other_mails: list[EmailStr] | None
    identities: list[GraphIdentity] | None

    class Config:
        orm_mode = True
