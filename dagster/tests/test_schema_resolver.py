from types import SimpleNamespace

import pytest
from src.utils.schema_resolver import (
    combine_schema_columns,
    resolve_schema_columns,
    resolve_schema_columns_for_upload,
)


def _column(name: str, *, primary_key: bool = False):
    return SimpleNamespace(name=name, primary_key=primary_key)


def _dataset(key: str, *, role: str = "member", group_id: str | None = "facilities"):
    return SimpleNamespace(
        id=f"{key}-id",
        key=key,
        role=role,
        group_id=group_id,
        current_version_id=f"{key}-current",
    )


class FakeRegistry:
    def __init__(self, datasets: dict, columns: dict):
        self.datasets = datasets
        self.columns = columns

    def get_dataset(self, key: str):
        return self.datasets[key]

    def get_version(self, dataset, version_number):
        return SimpleNamespace(
            id=f"{dataset.key}-{version_number or 'current'}",
            dataset_id=dataset.id,
            version=version_number or 1,
        )

    def get_columns(self, version):
        key = version.id.rsplit("-", 1)[0]
        return self.columns[key]

    def get_group_core(self, group_id: str):
        return next(
            (
                dataset
                for dataset in self.datasets.values()
                if dataset.group_id == group_id and dataset.role == "core"
            ),
            None,
        )


def test_combine_schema_columns_rejects_overlap():
    with pytest.raises(ValueError, match="latitude"):
        combine_schema_columns([_column("latitude")], [_column("latitude")])


def test_resolve_schema_columns_combines_current_core_and_member():
    registry = FakeRegistry(
        {
            "facility_core": _dataset("facility_core", role="core"),
            "school_geolocation": _dataset("school_geolocation"),
        },
        {
            "facility_core": [_column("latitude"), _column("longitude")],
            "school_geolocation": [_column("school_id_govt", primary_key=True)],
        },
    )

    columns = resolve_schema_columns(registry, "school_geolocation")

    assert [column.name for column in columns] == [
        "latitude",
        "longitude",
        "school_id_govt",
    ]


def test_resolve_schema_columns_for_upload_requires_version_pin():
    registry = FakeRegistry({}, {})
    upload = SimpleNamespace(
        id="upload-1",
        schema_key="school_geolocation",
        schema_version=None,
        core_schema_key=None,
        core_schema_version=None,
    )

    with pytest.raises(ValueError, match="no version pin"):
        resolve_schema_columns_for_upload(registry, "school_geolocation", upload)
