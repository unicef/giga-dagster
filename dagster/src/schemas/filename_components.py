from datetime import datetime

from pydantic import BaseModel, Field, constr


class FilenameComponents(BaseModel):
    id: str = Field(None)
    country_code: constr(min_length=3, max_length=3)
    dataset_type: str
    source: str = Field(None)
    timestamp: datetime = Field(None)
    rest: str = Field(None)
