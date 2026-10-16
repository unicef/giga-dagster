from dagster import AssetSelection, define_asset_job

from src.facilities import HEALTH_SPEC
from src.hooks.school_master import (
    school_dq_checks_location_db_update_hook,
    school_dq_overall_location_db_update_hook,
    school_ingest_error_db_update_hook,
)
from src.settings import settings

health_facility_geolocation__automated_data_checks_job = define_asset_job(
    name=HEALTH_SPEC.jobs.automated_checks,
    selection=[f"{HEALTH_SPEC.assets.raw}*"],
    hooks={
        school_dq_checks_location_db_update_hook,
        school_dq_overall_location_db_update_hook,
        school_ingest_error_db_update_hook,
    },
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

health_facility_geolocation__post_manual_checks_job = define_asset_job(
    name=HEALTH_SPEC.jobs.post_approval,
    selection=AssetSelection.keys(
        HEALTH_SPEC.assets.approved_rows,
        HEALTH_SPEC.assets.rejected_rows,
        HEALTH_SPEC.assets.silver,
        HEALTH_SPEC.assets.core,
        HEALTH_SPEC.assets.extension,
        f"{HEALTH_SPEC.assets.raw.removesuffix('_raw')}_publish_complete",
    ),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

health_facility_geolocation__admin_delete_rows_job = define_asset_job(
    name=HEALTH_SPEC.jobs.delete,
    selection=AssetSelection.keys(
        f"{HEALTH_SPEC.assets.raw.removesuffix('_raw')}_delete_staging"
    ),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
