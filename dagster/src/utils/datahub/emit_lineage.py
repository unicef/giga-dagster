import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from src.settings import settings
from src.utils.datahub.emit_dataset_metadata import create_dataset_urn

from dagster import InputContext


def emit_lineage(
    context: InputContext, dataset_filepath: str, upstream_filepath: str, platform: str
):
    datahub_emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )
    step = context.asset_key.to_user_string()
    if "raw" not in step:
        upstream_dataset_urn = create_dataset_urn(
            filepath=upstream_filepath, platform=platform
        )
        dataset_urn = create_dataset_urn(filepath=dataset_filepath, platform=platform)
        lineage_mce = builder.make_lineage_mce(
            [upstream_dataset_urn],  # Upstream URNs
            dataset_urn,  # Downstream URN
        )
        context.log.info(f"dataset_urn: {dataset_urn}")
        context.log.info(f"upstream_dataset_urn: {upstream_dataset_urn}")
        context.log.info(f"lineage_mce: {lineage_mce}")
        datahub_emitter.emit_mce(lineage_mce)
