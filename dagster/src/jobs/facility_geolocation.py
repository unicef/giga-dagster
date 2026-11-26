from dagster import AssetSelection, define_asset_job
from src.facility_profiles import load_all_facility_specs
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


# Discovered by load_jobs_from_package_module via this list. Do not also assign
# per-profile JobDefinition aliases at module level — that duplicates registration
# and can leave None attrs when a profile is absent.
FACILITY_GEOLOCATION_JOBS = []
FACILITY_JOBS_BY_SCHEMA_KEY: dict[str, dict[str, object]] = {}

for _schema_key, _spec in load_all_facility_specs().items():
    automated = _automated_job(_spec)
    post = _post_approval_job(_spec)
    delete = _delete_job(_spec)
    FACILITY_GEOLOCATION_JOBS.extend([automated, post, delete])
    FACILITY_JOBS_BY_SCHEMA_KEY[_schema_key] = {
        "automated": automated,
        "post_approval": post,
        "delete": delete,
    }
