import json
from datetime import datetime
from typing import Optional

from pydantic import Field, field_validator

from dagster import Config


class FileUploadConfig(Config):
    id: str
    created: str
    uploader_id: str
    uploader_email: str
    dq_report_path: Optional[str] = Field(default=None)
    country: str
    dataset: str
    source: Optional[str] = Field(default=None)
    original_filename: str
    column_to_schema_mapping: dict[str, str]
    column_license: dict[str, str]
    upload_path: str

    model_config = {"from_attributes": True}

    @field_validator("created", mode="before")
    @classmethod
    def parse_created(cls, v: datetime):
        return v.isoformat()

    @field_validator("column_to_schema_mapping", mode="before")
    @classmethod
    def parse_column_to_schema_mapping(cls, v: str | dict):
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator("column_license", mode="before")
    @classmethod
    def parse_column_license(cls, v: str | dict):
        if isinstance(v, str):
            return json.loads(v)
        return v
