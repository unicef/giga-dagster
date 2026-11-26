import sys

from src.assets.facility_geolocation.assets import SCHOOL_FACILITY_GEOLOCATION_ASSETS
from src.assets.school_geolocation_api.assets import school_geolocation_emis_api_mng
from src.facilities import SCHOOL_SPEC
from src.jobs.facility_geolocation import FACILITY_JOBS_BY_SCHEMA_KEY
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
    prefix = SCHOOL_SPEC.assets.raw.removesuffix("_raw")
    assert SCHOOL_SPEC.assets.raw in keys
    assert SCHOOL_SPEC.assets.dq_results in keys
    assert f"{prefix}_dq_duplicates_report" in keys
    assert SCHOOL_SPEC.assets.silver in keys
    assert SCHOOL_SPEC.assets.core in keys
    assert SCHOOL_SPEC.assets.extension in keys
    assert (
        school_geolocation_emis_api_mng.key.to_user_string()
        == "school_geolocation_emis_api_mng"
    )


def test_school_geolocation_job_names_and_selections_are_stable():
    automated = FACILITY_JOBS_BY_SCHEMA_KEY["schools"]["automated"]
    post = FACILITY_JOBS_BY_SCHEMA_KEY["schools"]["post_approval"]
    delete = FACILITY_JOBS_BY_SCHEMA_KEY["schools"]["delete"]
    prefix = SCHOOL_SPEC.assets.raw.removesuffix("_raw")

    assert automated.name == SCHOOL_SPEC.jobs.automated_checks
    assert automated.selection == (
        AssetSelection.keys(SCHOOL_SPEC.assets.raw).downstream(depth=sys.maxsize)
    )

    assert post.name == SCHOOL_SPEC.jobs.post_approval
    assert post.selection == (
        AssetSelection.keys(
            SCHOOL_SPEC.assets.approved_rows,
            SCHOOL_SPEC.assets.rejected_rows,
            SCHOOL_SPEC.assets.silver,
            SCHOOL_SPEC.assets.core,
            SCHOOL_SPEC.assets.extension,
            f"{prefix}_publish_complete",
        )
    )

    assert delete.name == SCHOOL_SPEC.jobs.delete
    assert delete.selection == (AssetSelection.keys(f"{prefix}_delete_staging"))

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
    # Facility school assets must not reuse bare legacy geolocation_* keys.
    assert "geolocation_raw" not in keys
    assert SCHOOL_SPEC.assets.raw.startswith("facility_geolocation_schools_")
