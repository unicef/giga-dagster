from dagster import define_asset_job
from src.settings import settings

school_connectivity__new_realtime_schools_job = define_asset_job(
    name="school_connectivity__new_realtime_schools_job",
    selection=["school_connectivity_update_realtime_schools_table*"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)


school_connectivity__update_schools_realtime_status_job = define_asset_job(
    name="school_connectivity__update_schools_realtime_status_job",
    selection=["school_connectivity_update_realtime_schools_status*"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
