from dagster import define_asset_job
from src.settings import settings

school_connectivity__get_new_realtime_schools_job = define_asset_job(
    name="school_connectivity__get_new_realtime_schools_job",
    selection=["school_connectivity_realtime_schools*"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)


school_connectivity__update_master_realtime_schools_job = define_asset_job(
    name="school_connectivity__update_master_realtime_schools_job",
    selection=["school_connectivity_realtime_silver*"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
