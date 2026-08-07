from typing import Literal

from pydantic import UUID4, BaseModel, ConfigDict, EmailStr


class GraphGroup(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID4
    description: str | None
    display_name: str


class GraphIdentity(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    issuer: str
    issuer_assigned_id: str | None
    sign_in_type: str


class GraphUser(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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


class DatabaseUser(BaseModel):
    id: str
    email: str
    given_name: str | None = None
    surname: str | None = None
    enabled: bool = True


class DatabaseRole(BaseModel):
    id: str
    name: str


class DatabaseRoleWithMembers(BaseModel):
    id: str
    name: str
    users: list[DatabaseUser]


class DatabaseUserWithRoles(DatabaseUser):
    roles: list[DatabaseRole]


class DatabaseUserCreateRequest(BaseModel):
    given_name: str
    surname: str
    email: EmailStr
    roles: list[DatabaseRole]
