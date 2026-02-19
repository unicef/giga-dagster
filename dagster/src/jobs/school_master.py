from dagster import AssetSelection, define_asset_job
from src.assets.common import GROUP_NAME as COMMON_GROUP_NAME
from src.hooks.school_master import (
    school_dq_checks_location_db_update_hook,
    school_dq_overall_location_db_update_hook,
    school_ingest_error_db_update_hook,
)
from src.hooks.slack_notification import slack_error_notification_hook
from src.settings import settings

school_master_geolocation__automated_data_checks_job = define_asset_job(
    name="school_master_geolocation__automated_data_checks_job",
    selection=["geolocation_raw*"],
    hooks={
        school_dq_checks_location_db_update_hook,
        school_dq_overall_location_db_update_hook,
        school_ingest_error_db_update_hook,
        slack_error_notification_hook,
    },
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

school_master_coverage__automated_data_checks_job = define_asset_job(
    name="school_master_coverage__automated_data_checks_job",
    selection=["coverage_raw*"],
    hooks={
        school_dq_checks_location_db_update_hook,
        school_dq_overall_location_db_update_hook,
        school_ingest_error_db_update_hook,
        slack_error_notification_hook,
    },
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)


school_master_geolocation__post_manual_checks_job = define_asset_job(
    name="school_master_geolocation__post_manual_checks_job",
    selection=AssetSelection.groups(COMMON_GROUP_NAME),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)


school_master_coverage__post_manual_checks_job = define_asset_job(
    name="school_master_coverage__post_manual_checks_job",
    selection=AssetSelection.groups(COMMON_GROUP_NAME),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

school_master_geolocation__admin_delete_rows_job = define_asset_job(
    name="school_master_geolocation__admin_delete_rows_job",
    selection="geolocation_delete_staging",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

school_master_coverage__admin_delete_rows_job = define_asset_job(
    name="school_master_coverage__admin_delete_rows_job",
    selection="coverage_delete_staging",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
