from dagster import define_asset_job

debug__drop_schema_job = define_asset_job(
    name="debug__drop_schema_job",
    selection=["debug__drop_schema"],
    tags={"dagster/priority": "99"},
)


debug__drop_table_job = define_asset_job(
    name="debug__drop_table_job",
    selection=["debug__drop_table"],
    tags={"dagster/priority": "99"},
)
