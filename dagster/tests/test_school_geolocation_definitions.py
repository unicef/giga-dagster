import sys

from dagster import AssetKey, AssetSelection

from src.assets.school_geolocation.assets import (
    geolocation_data_quality_results,
    geolocation_raw,
)
from src.assets.school_geolocation_api.assets import school_geolocation_emis_api_mng
from src.jobs.school_geolocation_api import (
    school_geolocation_api__get_mongolia_school_updates,
)
from src.jobs.school_master import (
    school_master_geolocation__admin_delete_rows_job,
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__post_manual_checks_job,
)


def test_school_geolocation_asset_names_are_stable():
    assert geolocation_raw.key.to_user_string() == "geolocation_raw"
    assert geolocation_data_quality_results.keys == {
        AssetKey("geolocation_data_quality_results"),
        AssetKey("geolocation_dq_duplicates_report"),
    }
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
        AssetSelection.groups("common")
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
