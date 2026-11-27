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

    core_table: str
    extension_table: str
    staging_table: str
    silver_table: str
    rejected_table: str
    error_table: str
    metadata_table: str


class FacilityMetadataField(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    label: str
    helper_text: str = ""
    type: str  # text | email | year | enum | month_year
    required: bool = False
    options: list[str] | None = None
    min_year: int | None = None

    @field_validator("type", mode="after")
    @classmethod
    def _known_type(cls, value: str) -> str:
        allowed = {"text", "email", "year", "enum", "month_year"}
        if value not in allowed:
            raise ValueError(f"type must be one of {sorted(allowed)}")
        return value


class FacilityMetadataSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = ""
    fields: list[FacilityMetadataField]


class FacilityMetadataForm(BaseModel):
    model_config = ConfigDict(extra="forbid")

    heading: str = "Add Metadata"
    intro: list[str] = Field(default_factory=list)
    sections: list[FacilityMetadataSection]

    def pdf_field_defs(self) -> list[dict[str, str]]:
        """Flatten section fields to ``{name, label}`` rows for the DQ PDF."""
        return [
            {"name": field.name, "label": field.label}
            for section in self.sections
            for field in section.fields
        ]


class FacilityPortalSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approval_label: str
    approval_domain: str
    metadata: FacilityMetadataForm | None = None


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
