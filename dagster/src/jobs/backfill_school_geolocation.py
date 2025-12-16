from dagster import define_asset_job
from src.settings import settings

backfill__school_geolocation_metadata_job = define_asset_job(
    name="backfill__school_geolocation_metadata_job",
    selection=["backfill_school_geolocation_metadata"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
