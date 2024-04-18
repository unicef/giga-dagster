from datetime import datetime
from typing import Generic, TypeVar

from pydantic import BaseModel

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
