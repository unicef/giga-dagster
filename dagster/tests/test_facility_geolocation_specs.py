import pytest

from src.assets.facility_geolocation import publish_factory
from src.assets.facility_geolocation.assets import (
    HEALTH_FACILITY_GEOLOCATION_ASSETS,
)
from src.facilities import HEALTH_SPEC, SCHOOL_SPEC, generate_facility_id


def test_school_spec_preserves_existing_runtime_contracts():
    assert SCHOOL_SPEC.assets.raw == "geolocation_raw"
    assert SCHOOL_SPEC.assets.bronze == "geolocation_bronze"
    assert SCHOOL_SPEC.assets.dq_results == "geolocation_data_quality_results"
    assert SCHOOL_SPEC.paths.upload_directory == "school-geolocation"
    assert (
        SCHOOL_SPEC.jobs.automated_checks
        == "school_master_geolocation__automated_data_checks_job"
    )
    assert SCHOOL_SPEC.dq_config_table == "SchoolGeolocationMasterDQChecks"
    assert SCHOOL_SPEC.approval_label == "School Geolocation"


def test_health_spec_uses_shared_core_and_health_extension():
    assert HEALTH_SPEC.tables.core_schema == "facility_core"
    assert HEALTH_SPEC.tables.extension_schema == "health_facility_details"
    assert HEALTH_SPEC.giga_id_column == "facility_id_giga"
    assert HEALTH_SPEC.govt_id_column == "facility_id_govt"
    assert set(HEALTH_SPEC.columns.core).isdisjoint(HEALTH_SPEC.columns.extension)


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


def test_health_jobs_and_sensors_load_in_repository():
    from src.definitions import defs

    repository = defs.get_repository_def()
    assert repository.get_job(HEALTH_SPEC.jobs.automated_checks)
    assert repository.get_job(HEALTH_SPEC.jobs.post_approval)
    assert repository.get_job(HEALTH_SPEC.jobs.delete)

    sensor_names = {sensor.name for sensor in repository.sensor_defs}
    assert "health_facility_geolocation__raw_file_uploads_sensor" in sensor_names
    assert "health_facility_geolocation__post_manual_checks_sensor" in sensor_names
    assert "health_facility_geolocation__admin_delete_rows_sensor" in sensor_names


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
