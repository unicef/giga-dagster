from dagster import define_asset_job

unstructured__emit_metadata_to_datahub_job = define_asset_job(
    name="unstructured__emit_metadata_to_datahub_job",
    selection=[
        "unstructured_raw",
    ],
)
