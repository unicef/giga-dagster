from dagster import AssetSelection, define_asset_job
from src.assets.unstructured import GROUP_NAME as UNSTRUCTURED_DATA_GROUP_NAME

unstructured__emit_metadata_to_datahub_job = define_asset_job(
    name="unstructured__emit_metadata_to_datahub_job",
    selection=AssetSelection.groups(UNSTRUCTURED_DATA_GROUP_NAME),
)
