from datetime import datetime

from pydantic import BaseModel


class MlabSchools(BaseModel):
    created: datetime
    school_id_govt: int
    source: str
    country_code: str

    model_config = {"from_attributes": True}


class GigaMeterSchools(BaseModel):
    created: datetime
    school_id_giga: str
    school_id_govt: int
    source: str

    model_config = {"from_attributes": True}


class RTSchools(BaseModel):
    school_id_giga: str
    school_id_govt: str
    realtime_date: datetime
    country_code: str
    country: str

    model_config = {"from_attributes": True}
