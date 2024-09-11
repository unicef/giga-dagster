from dagster import define_asset_job
from src.settings import settings

debug__drop_schema_job = define_asset_job(
    name="debug__drop_schema_job",
    selection=["debug__drop_schema"],
    tags={
        "dagster/priority": "99",
        "dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME,
    },
)


debug__drop_table_job = define_asset_job(
    name="debug__drop_table_job",
    selection=["debug__drop_table"],
    tags={
        "dagster/priority": "99",
        "dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME,
    },
)
