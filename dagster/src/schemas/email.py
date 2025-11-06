from datetime import datetime
from typing import Generic, Optional, TypeVar

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
    html_part: Optional[str] = Field(default=None)
    text_part: Optional[str] = Field(default=None)
