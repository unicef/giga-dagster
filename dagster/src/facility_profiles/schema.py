"""Pydantic schema for ADLS facility profile YAML files."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FacilityIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    govt_id_column: str
    giga_id_column: str
    name_column: str
    level_column: str
    country_column: str


class FacilityPaths(BaseModel):
    model_config = ConfigDict(extra="forbid")

    upload_directory: str
    dq_directory: str
    approval_directory: str
    delete_directory: str


class FacilityTables(BaseModel):
    model_config = ConfigDict(extra="forbid")

    core_schema: str
    core_table: str
    extension_schema: str
    extension_table: str
    staging_table: str
    silver_schema: str
    rejected_schema: str
    error_table: str


class FacilityPortalSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approval_label: str
    approval_domain: str


class FacilityDagsterSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    domain: str
    group_name: str
    dq_config_table: str
    metastore_schema_name: str | None = None
    enable_connectivity: bool = False
    enable_registration_updates: bool = False


class FacilityProfile(BaseModel):
    """One facility type. ``schema_key`` comes from the YAML filename stem."""

    model_config = ConfigDict(extra="forbid")

    schema_key: str = Field(exclude=True)
    facility_type: str
    dataset_type: str
    identity: FacilityIdentity
    paths: FacilityPaths
    tables: FacilityTables
    portal: FacilityPortalSection
    dagster: FacilityDagsterSection

    @field_validator("facility_type", "dataset_type", mode="after")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value or value.strip() != value:
            raise ValueError("must be a non-empty, trimmed string")
        return value
