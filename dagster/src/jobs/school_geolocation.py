from dagster import define_asset_job
from src.settings import settings

apply_fuzzy_corrections_job = define_asset_job(
    name="apply_fuzzy_corrections_job",
    selection="apply_fuzzy_corrections",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
