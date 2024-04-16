from pydantic import UUID4, BaseModel, EmailStr


class GraphUser(BaseModel):
    id: UUID4
    account_enabled: bool | None
    given_name: str | None
    surname: str | None
    mail: EmailStr | None
    display_name: str | None
    user_principal_name: EmailStr
