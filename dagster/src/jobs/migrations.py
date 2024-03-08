from dagster import define_asset_job

migrate__schema = define_asset_job(
    name="migrate__schema",
    selection=[
        "initialize_metaschema",
        "migrate_schema",
    ],
)
