import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.op_config import FileConfig


def emit_lineage(context: OpExecutionContext) -> None:
    datahub_emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )

    step = context.asset_key.to_user_string()
    context.log.info(f"step: {step}")

    if "raw" not in step:
        config = FileConfig(**context.get_step_execution_context().op_config)

        upstream_dataset_urn = config.datahub_source_dataset_urn
        context.log.info(f"upstream_dataset_urn: {upstream_dataset_urn}")

        dataset_urn = config.datahub_destination_dataset_urn
        context.log.info(f"dataset_urn: {dataset_urn}")

        lineage_mce = builder.make_lineage_mce(
            [upstream_dataset_urn],  # Upstream URNs
            dataset_urn,  # Downstream URN
        )
        context.log.info(f"lineage_mce: {lineage_mce}")

        context.log.info("EMITTING LINEAGE")
        datahub_emitter.emit_mce(lineage_mce)
        context.log.info("SUCCESS. LINEAGE EMITTED")

    else:
        context.log.info("NO LINEAGE SINCE RAW STEP HAS NO UPSTREAM DATASETS.")
