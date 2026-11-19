from dagster import AssetKey, define_asset_job
from src.settings import settings

maintenance__optimize_incremental_tables_job = define_asset_job(
    name="maintenance__optimize_incremental_tables_job",
    selection=[AssetKey(["maintenance", "optimize_incremental_tables"])],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
