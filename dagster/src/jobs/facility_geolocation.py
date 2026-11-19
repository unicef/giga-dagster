from dagster import AssetSelection, define_asset_job
from src.facilities import HEALTH_SPEC, SCHOOL_SPEC
from src.hooks.school_master import (
    school_dq_checks_location_db_update_hook,
    school_dq_overall_location_db_update_hook,
    school_ingest_error_db_update_hook,
)
from src.settings import settings

_DQ_HOOKS = {
    school_dq_checks_location_db_update_hook,
    school_dq_overall_location_db_update_hook,
    school_ingest_error_db_update_hook,
}


def _automated_job(spec):
    return define_asset_job(
        name=spec.jobs.automated_checks,
        selection=[f"{spec.assets.raw}*"],
        hooks=_DQ_HOOKS,
        tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
    )


def _post_approval_job(spec):
    prefix = spec.assets.raw.removesuffix("_raw")
    return define_asset_job(
        name=spec.jobs.post_approval,
        selection=AssetSelection.keys(
            spec.assets.approved_rows,
            spec.assets.rejected_rows,
            spec.assets.silver,
            spec.assets.core,
            spec.assets.extension,
            f"{prefix}_publish_complete",
        ),
        tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
    )


def _delete_job(spec):
    prefix = spec.assets.raw.removesuffix("_raw")
    return define_asset_job(
        name=spec.jobs.delete,
        selection=AssetSelection.keys(f"{prefix}_delete_staging"),
        tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
    )


school_master_geolocation__automated_data_checks_job = _automated_job(SCHOOL_SPEC)
school_master_geolocation__post_manual_checks_job = _post_approval_job(SCHOOL_SPEC)
school_master_geolocation__admin_delete_rows_job = _delete_job(SCHOOL_SPEC)

health_facility_geolocation__automated_data_checks_job = _automated_job(HEALTH_SPEC)
health_facility_geolocation__post_manual_checks_job = _post_approval_job(HEALTH_SPEC)
health_facility_geolocation__admin_delete_rows_job = _delete_job(HEALTH_SPEC)
