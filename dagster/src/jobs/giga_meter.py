from dagster import RetryPolicy, define_asset_job
from src.assets.giga_meter.assets import connectivity_ping_checks
from src.settings import settings

giga_meter_connectivity_ping_checks = define_asset_job(
    name="gigameter_parquet_to_delta_job",
    selection=[connectivity_ping_checks],
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 4}}}},
    op_retry_policy=RetryPolicy(max_retries=3, delay=60),
)

giga_meter__mlab_traceroutes_job = define_asset_job(
    name="giga_meter__mlab_traceroutes",
    selection=["mlab_traceroutes"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
