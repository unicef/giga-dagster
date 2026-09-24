from dagster import define_asset_job
from src.settings import settings

mlab_traceroutes__pull_measurements = define_asset_job(
    name="mlab_traceroutes__pull_measurements",
    selection=["mlab_traceroutes"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
