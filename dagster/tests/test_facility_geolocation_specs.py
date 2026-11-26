from pathlib import Path

import pytest
from src.assets.facility_geolocation import publish_factory
from src.assets.facility_geolocation.assets import (
    FACILITY_GEOLOCATION_ASSETS,
    HEALTH_FACILITY_GEOLOCATION_ASSETS,
    SCHOOL_FACILITY_GEOLOCATION_ASSETS,
)
from src.facilities import HEALTH_SPEC, SCHOOL_SPEC, generate_facility_id
from src.facility_profiles import derive_asset_names, derive_job_names
from src.sensors.facility_geolocation import schema_key_from_upload_path


def test_school_spec_loads_from_facility_profile():
    assert SCHOOL_SPEC.paths.upload_directory == "facility-geolocation"
    assert SCHOOL_SPEC.paths.schema_keys == ("schools",)
    assert SCHOOL_SPEC.paths.dq_directory == "school-geolocation"
    assert SCHOOL_SPEC.assets == derive_asset_names("schools")
    assert SCHOOL_SPEC.jobs == derive_job_names("schools")
    assert SCHOOL_SPEC.dq_config_table == "SchoolGeolocationMasterDQChecks"
    assert SCHOOL_SPEC.approval_label == "School Geolocation"
    assert SCHOOL_SPEC.govt_id_column == "facility_id_govt"
    assert SCHOOL_SPEC.giga_id_column == "facility_id_giga"
    assert SCHOOL_SPEC.name_column == "facility_name"
    assert SCHOOL_SPEC.level_column == "facility_level"
    assert {"geo_latitude", "geo_longitude"}.issubset(SCHOOL_SPEC.columns.core)
    assert "computer_lab_availability" in SCHOOL_SPEC.columns.extension
    assert "students_num" in SCHOOL_SPEC.columns.extension


def test_facility_staging_and_error_tables_are_shared_across_countries():
    assert (
        SCHOOL_SPEC.tables.error_table_full_name
        == "pipeline_tables.school_facility_geolocation_error_table"
    )
    assert (
        HEALTH_SPEC.tables.error_table_full_name
        == "pipeline_tables.health_facility_geolocation_error_table"
    )
    assert (
        SCHOOL_SPEC.tables.staging_table_full_name
        == "pipeline_tables.school_facility_geolocation_staging"
    )
    assert (
        HEALTH_SPEC.tables.staging_table_full_name
        == "pipeline_tables.health_facility_geolocation_staging"
    )


def test_health_spec_uses_shared_core_and_health_extension():
    assert HEALTH_SPEC.tables.core_schema == "facility_core"
    assert HEALTH_SPEC.tables.extension_schema == "health_facility_details"
    assert HEALTH_SPEC.giga_id_column == "facility_id_giga"
    assert HEALTH_SPEC.govt_id_column == "facility_id_govt"
    assert HEALTH_SPEC.paths.upload_directory == "facility-geolocation"
    assert HEALTH_SPEC.paths.schema_keys == ("health_facilities",)
    assert set(HEALTH_SPEC.columns.core).isdisjoint(HEALTH_SPEC.columns.extension)
    assert {"geo_latitude", "geo_longitude"}.issubset(HEALTH_SPEC.columns.core)
    assert "facility_health_type_govt" in HEALTH_SPEC.columns.extension
    assert "outpatients_avg" in HEALTH_SPEC.columns.extension
    assert "bed_count" not in HEALTH_SPEC.columns.extension


def test_facility_id_is_stable_namespaced_and_requires_identity():
    first = generate_facility_id("health", "UGA", "HC-001")
    second = generate_facility_id(" HEALTH ", "uga", " hc-001 ")

    assert first == second
    assert first != generate_facility_id("school", "UGA", "HC-001")
    assert first != generate_facility_id("health", "KEN", "HC-001")
    assert generate_facility_id("health", "UGA", None) is None


def test_health_asset_factory_creates_unique_asset_keys():
    keys = [
        key.to_user_string()
        for definition in HEALTH_FACILITY_GEOLOCATION_ASSETS
        for key in definition.keys
    ]

    assert len(keys) == len(set(keys))
    assert HEALTH_SPEC.assets.raw in keys
    assert HEALTH_SPEC.assets.silver in keys
    assert HEALTH_SPEC.assets.core in keys
    assert HEALTH_SPEC.assets.extension in keys


def test_school_and_health_factory_assets_have_disjoint_keys():
    school_keys = {
        key.to_user_string()
        for definition in SCHOOL_FACILITY_GEOLOCATION_ASSETS
        for key in definition.keys
    }
    health_keys = {
        key.to_user_string()
        for definition in HEALTH_FACILITY_GEOLOCATION_ASSETS
        for key in definition.keys
    }
    assert school_keys.isdisjoint(health_keys)
    all_keys = [
        key.to_user_string()
        for definition in FACILITY_GEOLOCATION_ASSETS
        for key in definition.keys
    ]
    assert len(all_keys) == len(set(all_keys))


def test_schema_key_from_upload_path_parses_facility_layout():
    path = Path(
        "raw/uploads/facility-geolocation/UGA/schools/"
        "upload-123_UGA_geolocation_20261105-120000.csv"
    )
    assert schema_key_from_upload_path(path, "facility-geolocation") == "schools"

    health_path = Path(
        "raw/uploads/facility-geolocation/BRA/health_facilities/"
        "upload-456_BRA_geolocation_20261105-120000.csv"
    )
    assert (
        schema_key_from_upload_path(health_path, "facility-geolocation")
        == "health_facilities"
    )


def test_specs_match_only_their_schema_keys():
    assert SCHOOL_SPEC.paths.matches_schema_key("schools")
    assert not SCHOOL_SPEC.paths.matches_schema_key("health_facilities")
    assert HEALTH_SPEC.paths.matches_schema_key("health_facilities")
    assert not HEALTH_SPEC.paths.matches_schema_key("schools")
    assert not SCHOOL_SPEC.paths.matches_schema_key(None)


def test_facility_jobs_and_sensors_are_registered():
    from src.jobs import facility_geolocation as facility_jobs
    from src.sensors import facility_geolocation as facility_sensors

    assert (
        facility_jobs.FACILITY_JOBS_BY_SCHEMA_KEY["schools"]["automated"].name
        == SCHOOL_SPEC.jobs.automated_checks
    )
    assert (
        facility_jobs.FACILITY_JOBS_BY_SCHEMA_KEY["schools"]["post_approval"].name
        == SCHOOL_SPEC.jobs.post_approval
    )
    assert (
        facility_jobs.FACILITY_JOBS_BY_SCHEMA_KEY["schools"]["delete"].name
        == SCHOOL_SPEC.jobs.delete
    )
    assert (
        facility_jobs.FACILITY_JOBS_BY_SCHEMA_KEY["health_facilities"]["automated"].name
        == HEALTH_SPEC.jobs.automated_checks
    )

    sensor_names = [
        sensor.name for sensor in facility_sensors.FACILITY_GEOLOCATION_SENSORS
    ]
    assert len(sensor_names) == len(set(sensor_names))
    assert not hasattr(facility_sensors, "delete_sensor")
    assert not hasattr(facility_jobs, "delete")
    sensor_names = set(sensor_names)
    assert sensor_names == {
        "facility_geolocation_schools__raw_file_uploads_sensor",
        "facility_geolocation_schools__post_manual_checks_sensor",
        "facility_geolocation_schools__admin_delete_rows_sensor",
        "facility_geolocation_health_facilities__raw_file_uploads_sensor",
        "facility_geolocation_health_facilities__post_manual_checks_sensor",
        "facility_geolocation_health_facilities__admin_delete_rows_sensor",
    }


def test_facility_approval_contract_rejects_ambiguous_payloads():
    valid = {
        "upload_id": "upload-1",
        "approved_change_ids": ["change-1"],
        "rejected_change_ids": ["change-2"],
        "approval_request_log_id": "log-1",
    }
    assert publish_factory._approval_parts(valid) == (
        "upload-1",
        ["change-1"],
        ["change-2"],
        "log-1",
    )

    with pytest.raises(ValueError, match="disjoint"):
        publish_factory._approval_parts(
            {
                **valid,
                "rejected_change_ids": ["change-1"],
            }
        )


def test_approval_parts_allows_reject_only_without_log_id():
    assert publish_factory._approval_parts(
        {
            "upload_id": "upload-1",
            "approved_change_ids": [],
            "rejected_change_ids": ["change-2"],
        }
    ) == ("upload-1", [], ["change-2"], None)


def test_approval_parts_requires_log_id_when_rows_are_approved():
    with pytest.raises(ValueError, match="approval_request_log_id"):
        publish_factory._approval_parts(
            {
                "upload_id": "upload-1",
                "approved_change_ids": ["change-1"],
                "rejected_change_ids": [],
            }
        )


def test_publish_steps_wait_for_upstream_tables_without_loading_them():
    from dagster import AssetKey

    assets = {
        asset.key.to_user_string(): asset
        for asset in publish_factory.build_facility_publish_assets(SCHOOL_SPEC)
    }
    core = assets[SCHOOL_SPEC.assets.core]
    extension = assets[SCHOOL_SPEC.assets.extension]
    prefix = SCHOOL_SPEC.assets.raw.removesuffix("_raw")
    complete = assets[f"{prefix}_publish_complete"]

    assert set(core.dependency_keys) == {
        AssetKey(SCHOOL_SPEC.assets.approved_rows),
        AssetKey(SCHOOL_SPEC.assets.silver),
    }
    assert list(core.keys_by_input_name) == ["approved"]
    assert set(extension.dependency_keys) == {
        AssetKey(SCHOOL_SPEC.assets.approved_rows),
        AssetKey(SCHOOL_SPEC.assets.core),
    }
    assert list(extension.keys_by_input_name) == ["approved"]
    assert set(complete.dependency_keys) == {
        AssetKey(SCHOOL_SPEC.assets.silver),
        AssetKey(SCHOOL_SPEC.assets.core),
        AssetKey(SCHOOL_SPEC.assets.extension),
        AssetKey(SCHOOL_SPEC.assets.rejected_rows),
    }
    assert complete.keys_by_input_name == {}
