from dagster import AssetSelection, define_asset_job
from src.assets.school_coverage import GROUP_NAME as COVERAGE_GROUP_NAME
from src.assets.school_geolocation import GROUP_NAME as GEOLOCATION_GROUP_NAME
from src.hooks.school_master import school_dq_checks_location_db_update_hook

school_master_geolocation__automated_data_checks_job = define_asset_job(
    name="school_master_geolocation__automated_data_checks_job",
    selection=AssetSelection.groups(GEOLOCATION_GROUP_NAME),
    hooks={school_dq_checks_location_db_update_hook},
)

school_master_coverage__automated_data_checks_job = define_asset_job(
    name="school_master_coverage__automated_data_checks_job",
    selection=AssetSelection.groups(COVERAGE_GROUP_NAME),
    hooks={school_dq_checks_location_db_update_hook},
)


school_master_geolocation__successful_manual_checks_job = define_asset_job(
    name="school_master_geolocation__successful_manual_checks_job",
    selection=["silver*"],
)


school_master_geolocation__failed_manual_checks_job = define_asset_job(
    name="school_master_geolocation__failed_manual_checks_job",
    selection="manual_review_failed_rows",
)


school_master_coverage__successful_manual_checks_job = define_asset_job(
    name="school_master_coverage__successful_manual_checks_job",
    selection=["silver*"],
)


school_master_coverage__failed_manual_checks_job = define_asset_job(
    name="school_master_coverage__failed_manual_checks_job",
    selection="manual_review_failed_rows*",
)
