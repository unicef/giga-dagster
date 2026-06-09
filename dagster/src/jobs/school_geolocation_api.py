from dagster import define_asset_job
from src.settings import settings

school_geolocation_api__get_mongolia_school_updates = define_asset_job(
    name="school_geolocation_api__get_mongolia_school_updates",
    selection=["school_geolocation_emis_api_mng"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
