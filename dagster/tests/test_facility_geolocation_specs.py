from pathlib import Path

import pytest
from src.assets.facility_geolocation import publish_factory
from src.assets.facility_geolocation.assets import (
    FACILITY_GEOLOCATION_ASSETS,
    HEALTH_FACILITY_GEOLOCATION_ASSETS,
    SCHOOL_FACILITY_GEOLOCATION_ASSETS,
)
from src.facilities import HEALTH_SPEC, SCHOOL_SPEC, generate_facility_id
from src.sensors.facility_geolocation import schema_key_from_upload_path


def test_school_spec_preserves_existing_runtime_contracts():
    assert SCHOOL_SPEC.assets.raw == "geolocation_raw"
    assert SCHOOL_SPEC.assets.bronze == "geolocation_bronze"
    assert SCHOOL_SPEC.assets.dq_results == "geolocation_data_quality_results"
    assert SCHOOL_SPEC.paths.upload_directory == "facility-geolocation"
    assert SCHOOL_SPEC.paths.schema_keys == ("schools",)
    assert SCHOOL_SPEC.paths.dq_directory == "school-geolocation"
    assert (
        SCHOOL_SPEC.jobs.automated_checks
        == "school_master_geolocation__automated_data_checks_job"
    )
    assert SCHOOL_SPEC.dq_config_table == "SchoolGeolocationMasterDQChecks"
    assert SCHOOL_SPEC.approval_label == "School Geolocation"
    assert SCHOOL_SPEC.govt_id_column == "facility_id_govt"
    assert SCHOOL_SPEC.giga_id_column == "facility_id_giga"
    assert SCHOOL_SPEC.name_column == "facility_name"
    assert SCHOOL_SPEC.level_column == "facility_level"
    assert {"geo_latitude", "geo_longitude"}.issubset(SCHOOL_SPEC.columns.core)
    assert "computer_lab_availability" in SCHOOL_SPEC.columns.extension
    assert "students_num" in SCHOOL_SPEC.columns.extension


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
        facility_jobs.school_master_geolocation__automated_data_checks_job.name
        == SCHOOL_SPEC.jobs.automated_checks
    )
    assert (
        facility_jobs.school_master_geolocation__post_manual_checks_job.name
        == SCHOOL_SPEC.jobs.post_approval
    )
    assert (
        facility_jobs.school_master_geolocation__admin_delete_rows_job.name
        == SCHOOL_SPEC.jobs.delete
    )
    assert (
        facility_jobs.health_facility_geolocation__automated_data_checks_job.name
        == HEALTH_SPEC.jobs.automated_checks
    )
    assert (
        facility_jobs.health_facility_geolocation__post_manual_checks_job.name
        == HEALTH_SPEC.jobs.post_approval
    )
    assert (
        facility_jobs.health_facility_geolocation__admin_delete_rows_job.name
        == HEALTH_SPEC.jobs.delete
    )

    sensor_names = {
        facility_sensors.school_master_geolocation__raw_file_uploads_sensor.name,
        facility_sensors.school_master_geolocation__post_manual_checks_sensor.name,
        facility_sensors.school_master_geolocation__admin_delete_rows_sensor.name,
        facility_sensors.health_facility_geolocation__raw_file_uploads_sensor.name,
        facility_sensors.health_facility_geolocation__post_manual_checks_sensor.name,
        facility_sensors.health_facility_geolocation__admin_delete_rows_sensor.name,
    }
    assert sensor_names == {
        "school_master_geolocation__raw_file_uploads_sensor",
        "school_master_geolocation__post_manual_checks_sensor",
        "school_master_geolocation__admin_delete_rows_sensor",
        "health_facility_geolocation__raw_file_uploads_sensor",
        "health_facility_geolocation__post_manual_checks_sensor",
        "health_facility_geolocation__admin_delete_rows_sensor",
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
