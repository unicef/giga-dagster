from dagster import RetryPolicy, define_asset_job
from src.assets.giga_meter.assets import connectivity_ping_checks

giga_meter_connectivity_ping_checks = define_asset_job(
    name="giga_meter",
    selection=[connectivity_ping_checks],
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 4}}}},
    op_retry_policy=RetryPolicy(max_retries=3, delay=60),
)
