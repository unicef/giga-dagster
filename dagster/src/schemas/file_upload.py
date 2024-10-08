import json
from datetime import datetime

from pydantic import Field, validator

from dagster import Config


class FileUploadConfig(Config):
    id: str
    created: str
    uploader_id: str
    uploader_email: str
    dq_report_path: str = Field(None)
    country: str
    dataset: str
    source: str = Field(None)
    original_filename: str
    column_to_schema_mapping: dict[str, str]
    column_license: dict[str, str]
    upload_path: str

    class Config:
        orm_mode = True

    @validator("created", pre=True)
    def parse_created(cls, v: datetime):
        return v.isoformat()

    @validator("column_to_schema_mapping", pre=True)
    def parse_column_to_schema_mapping(cls, v: str | dict):
        if isinstance(v, str):
            return json.loads(v)
        return v

    @validator("column_license", pre=True)
    def parse_column_license(cls, v: str | dict):
        if isinstance(v, str):
            return json.loads(v)
        return v
