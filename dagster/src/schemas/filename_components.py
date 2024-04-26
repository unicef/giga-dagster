from datetime import datetime

from pydantic import BaseModel, Field, constr


class FilenameComponents(BaseModel):
    id: str = Field(None)
    country_code: constr(min_length=3, max_length=3, to_upper=True)
    dataset_type: str = Field(None)
    source: str = Field(None)
    timestamp: datetime = Field(None)
    rest: str = Field(None)
