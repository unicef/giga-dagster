import sys

from src.assets.facility_geolocation.assets import SCHOOL_FACILITY_GEOLOCATION_ASSETS
from src.assets.school_geolocation_api.assets import school_geolocation_emis_api_mng
from src.facilities import SCHOOL_SPEC
from src.jobs.facility_geolocation import (
    school_master_geolocation__admin_delete_rows_job,
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__post_manual_checks_job,
)
from src.jobs.school_geolocation_api import (
    school_geolocation_api__get_mongolia_school_updates,
)

from dagster import AssetSelection


def test_school_geolocation_factory_asset_names_are_stable():
    keys = {
        key.to_user_string()
        for definition in SCHOOL_FACILITY_GEOLOCATION_ASSETS
        for key in definition.keys
    }
    assert SCHOOL_SPEC.assets.raw in keys
    assert SCHOOL_SPEC.assets.dq_results in keys
    assert "geolocation_dq_duplicates_report" in keys
    assert SCHOOL_SPEC.assets.silver in keys
    assert SCHOOL_SPEC.assets.core in keys
    assert SCHOOL_SPEC.assets.extension in keys
    assert (
        school_geolocation_emis_api_mng.key.to_user_string()
        == "school_geolocation_emis_api_mng"
    )


def test_school_geolocation_job_names_and_selections_are_stable():
    assert (
        school_master_geolocation__automated_data_checks_job.name
        == "school_master_geolocation__automated_data_checks_job"
    )
    assert school_master_geolocation__automated_data_checks_job.selection == (
        AssetSelection.keys("geolocation_raw").downstream(depth=sys.maxsize)
    )

    assert (
        school_master_geolocation__post_manual_checks_job.name
        == "school_master_geolocation__post_manual_checks_job"
    )
    assert school_master_geolocation__post_manual_checks_job.selection == (
        AssetSelection.keys(
            SCHOOL_SPEC.assets.approved_rows,
            SCHOOL_SPEC.assets.rejected_rows,
            SCHOOL_SPEC.assets.silver,
            SCHOOL_SPEC.assets.core,
            SCHOOL_SPEC.assets.extension,
            "geolocation_publish_complete",
        )
    )

    assert (
        school_master_geolocation__admin_delete_rows_job.name
        == "school_master_geolocation__admin_delete_rows_job"
    )
    assert school_master_geolocation__admin_delete_rows_job.selection == (
        AssetSelection.keys("geolocation_delete_staging")
    )

    assert (
        school_geolocation_api__get_mongolia_school_updates.name
        == "school_geolocation_api__get_mongolia_school_updates"
    )
    assert school_geolocation_api__get_mongolia_school_updates.selection == (
        AssetSelection.keys("school_geolocation_emis_api_mng")
    )


def test_school_factory_assets_do_not_collide_with_common_keys():
    keys = {
        key.to_user_string()
        for definition in SCHOOL_FACILITY_GEOLOCATION_ASSETS
        for key in definition.keys
    }
    assert "silver" not in keys
    assert "manual_review_passed_rows" not in keys
    assert "manual_review_failed_rows" not in keys
    assert "master" not in keys
    assert "reference" not in keys
