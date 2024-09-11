from dagster import define_asset_job
from src.settings import settings

unstructured__emit_metadata_to_datahub_job = define_asset_job(
    name="unstructured__emit_metadata_to_datahub_job",
    selection="unstructured_raw",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

generalized_unstructured__emit_metadata_to_datahub_job = define_asset_job(
    name="generalized_unstructured__emit_metadata_to_datahub_job",
    selection="generalized_unstructured_raw",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
