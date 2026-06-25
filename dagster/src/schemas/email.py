from datetime import datetime
from typing import Generic, TypeVar

from pydantic import BaseModel, Field

DataT = TypeVar("DataT")


class EmailRenderRequest(BaseModel, Generic[DataT]):
    email: str
    props: DataT


class MasterDataReleaseNotificationRenderRequest(BaseModel):
    added: int
    country: str
    modified: int
    updateDate: datetime
    version: str
    rows: int


class GenericEmailRequest(BaseModel):
    recipients: list[str]
    subject: str
    html_part: str | None = Field(None)
    text_part: str | None = Field(None)
    attachments: list[dict] | None = Field(None)
