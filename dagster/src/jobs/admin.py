from dagster import define_asset_job

admin__terminate_all_runs_job = define_asset_job(
    name="admin__terminate_all_runs_job",
    selection=["admin__terminate_all_runs"],
    tags={"dagster/priority": "99"},
)

admin__drop_schema_job = define_asset_job(
    name="admin__drop_schema_job",
    selection=["admin__drop_schema"],
    tags={"dagster/priority": "99"},
)


admin__drop_table_job = define_asset_job(
    name="admin__drop_table_job",
    selection=["admin__drop_table"],
    tags={"dagster/priority": "99"},
)
