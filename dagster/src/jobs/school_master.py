from dagster import AssetSelection, define_asset_job
from src.assets.common import GROUP_NAME as COMMON_GROUP_NAME
from src.assets.school_coverage import GROUP_NAME as COVERAGE_GROUP_NAME
from src.assets.school_geolocation import GROUP_NAME as GEOLOCATION_GROUP_NAME
from src.hooks.school_master import (
    school_dq_checks_location_db_update_hook,
    school_dq_overall_location_db_update_hook,
    school_ingest_error_db_update_hook,
)

school_master_geolocation__automated_data_checks_job = define_asset_job(
    name="school_master_geolocation__automated_data_checks_job",
    selection=AssetSelection.groups(GEOLOCATION_GROUP_NAME),
    hooks={
        school_dq_checks_location_db_update_hook,
        school_dq_overall_location_db_update_hook,
        school_ingest_error_db_update_hook,
    },
)

school_master_coverage__automated_data_checks_job = define_asset_job(
    name="school_master_coverage__automated_data_checks_job",
    selection=AssetSelection.groups(COVERAGE_GROUP_NAME),
    hooks={
        school_dq_checks_location_db_update_hook,
        school_dq_overall_location_db_update_hook,
        school_ingest_error_db_update_hook,
    },
)


school_master_geolocation__post_manual_checks_job = define_asset_job(
    name="school_master_geolocation__post_manual_checks_job",
    selection=AssetSelection.groups(COMMON_GROUP_NAME),
)


school_master_coverage__post_manual_checks_job = define_asset_job(
    name="school_master_coverage__post_manual_checks_job",
    selection=AssetSelection.groups(COMMON_GROUP_NAME),
)
