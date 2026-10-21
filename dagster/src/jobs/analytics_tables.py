from dagster import AssetSelection, define_asset_job, multiprocess_executor
from src.settings import settings

analytics_tables_daily_job = define_asset_job(
    name="analytics_tables__daily_job",
    selection=AssetSelection.groups("daily"),
    executor_def=multiprocess_executor.configured(
        {
            "tag_concurrency_limits": [
                {"key": "trino_heavy", "value": "true", "limit": 1},
            ]
        }
    ),
    tags={
        "dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME,
        "giga/concurrency_group": "analytics_tables",
    },
)

analytics_tables_incremental_job = define_asset_job(
    name="analytics_tables__incremental_job",
    selection=AssetSelection.groups("incremental"),
    tags={
        "dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME,
        "giga/concurrency_group": "analytics_tables",
    },
)
