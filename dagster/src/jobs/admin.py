from dagster import define_asset_job

admin__terminate_all_runs_job = define_asset_job(
    name="admin__terminate_all_runs_job",
    selection=["admin__terminate_all_runs"],
    tags={"dagster/priority": "99"},
)
