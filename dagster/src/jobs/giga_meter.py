from dagster import define_asset_job
from src.assets.giga_meter.assets import connectivity_ping_checks

giga_meter_connectivity_ping_checks = define_asset_job(
    name="giga_meter", selection=[connectivity_ping_checks]
)
