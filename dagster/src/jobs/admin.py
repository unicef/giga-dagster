from dagster import define_asset_job
from src.settings import settings

admin__terminate_all_runs_job = define_asset_job(
    name="admin__terminate_all_runs_job",
    selection=["admin__terminate_all_runs"],
    tags={
        "dagster/priority": "99",
        "dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME,
    },
)

admin__create_lakehouse_local_job = define_asset_job(
    name="admin__create_lakehouse_local_job",
    selection=["admin__create_lakehouse_local"],
)
