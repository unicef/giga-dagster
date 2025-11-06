from datetime import datetime
from typing import Annotated, Optional

from pydantic import BaseModel, Field, StringConstraints


class FilenameComponents(BaseModel):
    id: Optional[str] = Field(default=None)
    country_code: Annotated[
        str, StringConstraints(min_length=3, max_length=3, to_upper=True)
    ]
    dataset_type: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)  # Allow None for geolocation files
    timestamp: Optional[datetime] = Field(default=None)
    rest: Optional[str] = Field(default=None)
