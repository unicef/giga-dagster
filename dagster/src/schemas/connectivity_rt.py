from datetime import datetime

from pydantic import BaseModel, ConfigDict


class MlabSchools(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    created: datetime
    school_id_govt: int
    source: str
    country_code: str


class GigaMeterSchools(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    created: datetime
    school_id_giga: str
    school_id_govt: int
    source: str


class RTSchools(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    school_id_giga: str
    school_id_govt: str
    realtime_date: datetime
    country_code: str
    country: str
