from dagster import define_asset_job

migrations__schema = define_asset_job(
    name="migrations__schema",
    selection=["migrate_schema"],
)
