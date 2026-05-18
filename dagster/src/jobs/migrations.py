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

add_default_value_to_metaschemas__job = define_asset_job(
    name="add_default_value_to_metaschemas__job",
    selection=["add_default_value_to_metaschemas"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

migrate__set_table_properties__job = define_asset_job(
    name="migrate__set_table_properties__job",
    selection=["migrate__set_table_properties"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

migrate__rename_column__job = define_asset_job(
    name="migrate__rename_column__job",
    selection=["migrate__rename_column"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

migrate__drop_column__job = define_asset_job(
    name="migrate__drop_column__job",
    selection=["migrate__drop_column"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
