from datetime import datetime

from pydantic import BaseModel


class MlabSchools(BaseModel):
    created: datetime
    school_id_govt: int
    source: str
    country_code: str

    class Config:
        orm_mode = True


class GigaMeterSchools(BaseModel):
    created: datetime
    school_id_giga: str
    school_id_govt: int
    source: str

    class Config:
        orm_mode = True


class RTSchools(BaseModel):
    school_id_giga: str
    school_id_govt: str
    realtime_date: datetime
    country_code: str
    country: str

    class Config:
        orm_mode = True
