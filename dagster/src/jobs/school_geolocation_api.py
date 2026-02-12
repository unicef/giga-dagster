from dagster import define_asset_job
from src.settings import settings

school_connectivity__get_new_realtime_schools_job = define_asset_job(
    name="school_geolocation_api__get_monogolia_school_updates",
    selection=["mng_school_geolocation_api_raw"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
