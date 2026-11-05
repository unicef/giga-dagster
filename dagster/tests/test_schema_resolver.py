from types import SimpleNamespace

import pytest

from src.utils.schema_resolver import (
    combine_schema_columns,
    primary_key_name,
    resolve_schema_columns,
    resolve_schema_columns_for_upload,
)


def _column(name: str, *, primary_key: bool | None = None, data_type: str = "string"):
    return SimpleNamespace(
        name=name,
        data_type=data_type,
        is_nullable=True,
        primary_key=primary_key,
        partition_order=None,
    )


class InMemorySchemaRegistry:
    def __init__(self):
        self.datasets: dict[str, SimpleNamespace] = {}
        self.versions: dict[str, list[SimpleNamespace]] = {}
        self.columns: dict[str, list[SimpleNamespace]] = {}

    def add_dataset(
        self,
        *,
        key: str,
        role: str,
        group_id: str | None,
        current_version: int,
        columns: list[SimpleNamespace],
        dataset_id: str | None = None,
    ) -> SimpleNamespace:
        dataset_id = dataset_id or f"{key}-id"
        version_id = f"{key}-v{current_version}"
        dataset = SimpleNamespace(
            id=dataset_id,
            key=key,
            role=role,
            group_id=group_id,
            current_version_id=version_id,
        )
        self.datasets[key] = dataset
        self.versions[dataset_id] = [
            SimpleNamespace(id=version_id, dataset_id=dataset_id, version=current_version)
        ]
        self.columns[version_id] = columns
        return dataset

    def get_dataset(self, key: str) -> SimpleNamespace:
        if key not in self.datasets:
            raise ValueError(f"Schema '{key}' is not registered")
        return self.datasets[key]

    def get_version(
        self, dataset: SimpleNamespace, version_number: int | None
    ) -> SimpleNamespace:
        versions = self.versions[dataset.id]
        if version_number is None:
            version = next(
                (item for item in versions if item.id == dataset.current_version_id),
                None,
            )
        else:
            version = next(
                (item for item in versions if item.version == version_number), None
            )
        if version is None:
            raise ValueError(
                f"Schema '{dataset.key}' version {version_number} was not found"
            )
        return version

    def get_columns(self, version: SimpleNamespace) -> list[SimpleNamespace]:
        return list(self.columns[version.id])

    def get_group_core(self, group_id: str) -> SimpleNamespace | None:
        return next(
            (
                dataset
                for dataset in self.datasets.values()
                if dataset.group_id == group_id and dataset.role == "core"
            ),
            None,
        )


def _facilities_registry() -> InMemorySchemaRegistry:
    registry = InMemorySchemaRegistry()
    registry.add_dataset(
        key="facility_core",
        role="core",
        group_id="facilities",
        current_version=2,
        columns=[
            _column("facility_id_giga", primary_key=True),
            _column("latitude", data_type="double"),
        ],
    )
    registry.add_dataset(
        key="school_geolocation",
        role="member",
        group_id="facilities",
        current_version=3,
        columns=[
            _column("school_id_govt"),
            _column("education_level_govt"),
        ],
    )
    return registry


def test_member_resolution_includes_current_core_columns():
    columns = resolve_schema_columns(_facilities_registry(), "school_geolocation")

    assert [column.name for column in columns] == [
        "facility_id_giga",
        "latitude",
        "school_id_govt",
        "education_level_govt",
    ]


def test_core_resolution_does_not_merge_itself():
    columns = resolve_schema_columns(_facilities_registry(), "facility_core")

    assert [column.name for column in columns] == ["facility_id_giga", "latitude"]


def test_duplicate_core_and_member_columns_are_rejected():
    registry = _facilities_registry()
    registry.columns["school_geolocation-v3"].append(_column("latitude"))

    with pytest.raises(ValueError, match="overlap"):
        resolve_schema_columns(registry, "school_geolocation")


def test_upload_pins_do_not_resolve_current_core():
    registry = _facilities_registry()
    file_upload = SimpleNamespace(
        id="upload-1",
        schema_key="school_geolocation",
        schema_version=3,
        core_schema_key=None,
        core_schema_version=None,
    )

    columns = resolve_schema_columns_for_upload(
        registry, "school_geolocation", file_upload
    )

    assert [column.name for column in columns] == [
        "school_id_govt",
        "education_level_govt",
    ]


def test_upload_pins_use_explicit_core_and_member_versions():
    registry = _facilities_registry()
    file_upload = SimpleNamespace(
        id="upload-1",
        schema_key="school_geolocation",
        schema_version=3,
        core_schema_key="facility_core",
        core_schema_version=2,
    )

    columns = resolve_schema_columns_for_upload(
        registry, "school_geolocation", file_upload
    )

    assert [column.name for column in columns] == [
        "facility_id_giga",
        "latitude",
        "school_id_govt",
        "education_level_govt",
    ]


def test_upload_without_schema_key_uses_current_resolved_schema():
    registry = _facilities_registry()
    file_upload = SimpleNamespace(
        id="upload-legacy",
        schema_key=None,
        schema_version=None,
        core_schema_key=None,
        core_schema_version=None,
    )

    columns = resolve_schema_columns_for_upload(
        registry, "school_geolocation", file_upload
    )

    assert [column.name for column in columns] == [
        "facility_id_giga",
        "latitude",
        "school_id_govt",
        "education_level_govt",
    ]


def test_combine_schema_columns_and_primary_key_helpers():
    core = [_column("facility_id_giga", primary_key=True)]
    member = [_column("school_id_govt")]

    combined = combine_schema_columns(core, member)

    assert primary_key_name(combined, "school_geolocation") == "facility_id_giga"
    with pytest.raises(ValueError, match="overlap"):
        combine_schema_columns(core, [_column("facility_id_giga")])
