from dataclasses import dataclass

from sqlalchemy import Boolean, Float, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base_database import BaseModel


@dataclass(frozen=True)
class SchemaVersionSelection:
    schema_key: str
    schema_version: int | None = None
    core_schema_key: str | None = None
    core_schema_version: int | None = None


class SchemaDataset(BaseModel):
    __tablename__ = "schema_datasets"

    key: Mapped[str] = mapped_column(unique=True, nullable=False)
    role: Mapped[str] = mapped_column(nullable=False)
    group_id: Mapped[str | None] = mapped_column(nullable=True)
    current_version_id: Mapped[str | None] = mapped_column(nullable=True)


class SchemaVersion(BaseModel):
    __tablename__ = "schema_versions"

    dataset_id: Mapped[str] = mapped_column(nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer(), nullable=False)


class SchemaVersionColumn(BaseModel):
    __tablename__ = "schema_version_columns"

    version_id: Mapped[str] = mapped_column(nullable=False, index=True)
    ordinal_position: Mapped[int] = mapped_column(Integer(), nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    data_type: Mapped[str] = mapped_column(nullable=False)
    is_nullable: Mapped[bool | None] = mapped_column(Boolean(), nullable=True)
    is_important: Mapped[bool | None] = mapped_column(Boolean(), nullable=True)
    is_system_generated: Mapped[bool | None] = mapped_column(Boolean(), nullable=True)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    primary_key: Mapped[bool | None] = mapped_column(Boolean(), nullable=True)
    partition_order: Mapped[int | None] = mapped_column(Integer(), nullable=True)
    license: Mapped[str | None] = mapped_column(Text(), nullable=True)
    units: Mapped[str | None] = mapped_column(nullable=True)
    hint: Mapped[str | None] = mapped_column(Text(), nullable=True)
    is_mandatory: Mapped[bool | None] = mapped_column(Boolean(), nullable=True)
    is_unique: Mapped[bool | None] = mapped_column(Boolean(), nullable=True)
    domain_values: Mapped[str | None] = mapped_column(Text(), nullable=True)
    range_min: Mapped[float | None] = mapped_column(Float(), nullable=True)
    range_max: Mapped[float | None] = mapped_column(Float(), nullable=True)
    dynamic_max: Mapped[str | None] = mapped_column(nullable=True)
    precision_min: Mapped[int | None] = mapped_column(Integer(), nullable=True)
    critical_for: Mapped[str | None] = mapped_column(nullable=True)
