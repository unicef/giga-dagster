from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints


class FilenameComponents(BaseModel):
    id: str | None = Field(None)
    country_code: Annotated[
        str, StringConstraints(min_length=3, max_length=3, to_upper=True)
    ]
    dataset_type: str | None = Field(None)
    source: str | None = Field(None)
    timestamp: datetime | None = Field(None)
    rest: str | None = Field(None)
