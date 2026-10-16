from typing import Protocol

from models.schema_registry import SchemaDataset, SchemaVersion, SchemaVersionColumn
from sqlalchemy import select
from sqlalchemy.orm import Session


class SchemaPinSource(Protocol):
    id: str
    schema_key: str | None
    schema_version: int | None
    core_schema_key: str | None
    core_schema_version: int | None


class SchemaRegistry(Protocol):
    def get_dataset(self, key: str) -> SchemaDataset: ...

    def get_version(
        self, dataset: SchemaDataset, version_number: int | None
    ) -> SchemaVersion: ...

    def get_columns(self, version: SchemaVersion) -> list[SchemaVersionColumn]: ...

    def get_group_core(self, group_id: str) -> SchemaDataset | None: ...


class PostgresSchemaRegistry:
    def __init__(self, db: Session):
        self.db = db

    def get_dataset(self, key: str) -> SchemaDataset:
        dataset = self.db.scalar(select(SchemaDataset).where(SchemaDataset.key == key))
        if dataset is None:
            raise ValueError(f"Schema '{key}' is not registered")
        return dataset

    def get_version(
        self, dataset: SchemaDataset, version_number: int | None
    ) -> SchemaVersion:
        query = select(SchemaVersion).where(SchemaVersion.dataset_id == dataset.id)
        if version_number is None:
            if dataset.current_version_id is None:
                raise ValueError(f"Schema '{dataset.key}' has no published version")
            query = query.where(SchemaVersion.id == dataset.current_version_id)
        else:
            query = query.where(SchemaVersion.version == version_number)
        version = self.db.scalar(query)
        if version is None:
            raise ValueError(
                f"Schema '{dataset.key}' version {version_number} was not found"
            )
        return version

    def get_columns(self, version: SchemaVersion) -> list[SchemaVersionColumn]:
        return list(
            self.db.scalars(
                select(SchemaVersionColumn)
                .where(SchemaVersionColumn.version_id == version.id)
                .order_by(SchemaVersionColumn.ordinal_position)
            )
        )

    def get_group_core(self, group_id: str) -> SchemaDataset | None:
        return self.db.scalar(
            select(SchemaDataset).where(
                SchemaDataset.group_id == group_id,
                SchemaDataset.role == "core",
            )
        )


def combine_schema_columns(
    core_columns: list[SchemaVersionColumn],
    member_columns: list[SchemaVersionColumn],
) -> list[SchemaVersionColumn]:
    duplicate_names = sorted(
        {column.name for column in core_columns}.intersection(
            column.name for column in member_columns
        )
    )
    if duplicate_names:
        raise ValueError(
            "Core/member schema columns overlap: " + ", ".join(duplicate_names)
        )
    return [*core_columns, *member_columns]


def _resolve_core_dataset(
    registry: SchemaRegistry,
    dataset: SchemaDataset,
    *,
    core_schema_key: str | None,
    resolve_current_core: bool,
) -> SchemaDataset | None:
    if dataset.role == "core":
        if core_schema_key is not None and core_schema_key != dataset.key:
            raise ValueError(
                f"Schema '{dataset.key}' is a core and cannot inherit '{core_schema_key}'"
            )
        return None
    if core_schema_key is not None:
        core = registry.get_dataset(core_schema_key)
        if core.group_id != dataset.group_id or core.role != "core":
            raise ValueError(
                f"Schema '{core_schema_key}' is not the core for '{dataset.key}'"
            )
        return core
    if resolve_current_core and dataset.group_id is not None:
        return registry.get_group_core(dataset.group_id)
    return None


def resolve_schema_columns(
    registry: SchemaRegistry,
    schema_name: str,
    *,
    schema_version: int | None = None,
    core_schema_key: str | None = None,
    core_schema_version: int | None = None,
    resolve_current_core: bool = True,
) -> list[SchemaVersionColumn]:
    dataset = registry.get_dataset(schema_name)
    member_columns = registry.get_columns(registry.get_version(dataset, schema_version))
    core = _resolve_core_dataset(
        registry,
        dataset,
        core_schema_key=core_schema_key,
        resolve_current_core=resolve_current_core,
    )
    core_columns: list[SchemaVersionColumn] = []
    if core is not None and (
        core_schema_version is not None or core.current_version_id is not None
    ):
        core_columns = registry.get_columns(
            registry.get_version(core, core_schema_version)
        )
    return combine_schema_columns(core_columns, member_columns)


def resolve_schema_columns_for_upload(
    registry: SchemaRegistry,
    fallback_schema_name: str,
    file_upload: SchemaPinSource,
) -> list[SchemaVersionColumn]:
    if file_upload.schema_key is None:
        return resolve_schema_columns(registry, fallback_schema_name)
    if file_upload.schema_version is None:
        raise ValueError(
            f"Upload '{file_upload.id}' has a schema key but no version pin"
        )
    return resolve_schema_columns(
        registry,
        file_upload.schema_key,
        schema_version=file_upload.schema_version,
        core_schema_key=file_upload.core_schema_key,
        core_schema_version=file_upload.core_schema_version,
        resolve_current_core=False,
    )


def primary_key_name(columns: list[SchemaVersionColumn], schema_name: str) -> str:
    primary_key = next((column.name for column in columns if column.primary_key), None)
    if primary_key is None:
        raise ValueError(f"Schema '{schema_name}' has no primary key")
    return primary_key
