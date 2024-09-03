from dagster import define_asset_job
from src.settings import settings

migrate__schema = define_asset_job(
    name="migrate__schema",
    selection=[
        "initialize_metaschema",
        "migrate_schema",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
