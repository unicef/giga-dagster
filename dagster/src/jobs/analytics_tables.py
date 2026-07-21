from dagster import AssetSelection, define_asset_job
from src.settings import settings

analytics_tables_daily_job = define_asset_job(
    name="analytics_tables__daily_job",
    selection=AssetSelection.groups("daily"),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

analytics_tables_incremental_job = define_asset_job(
    name="analytics_tables__incremental_job",
    selection=AssetSelection.groups("incremental"),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
