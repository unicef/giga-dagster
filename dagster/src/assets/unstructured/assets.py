import sentry_sdk
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emitter,
    define_dataset_properties,
)
from src.utils.op_config import FileConfig
from src.utils.sentry import log_op_context

from dagster import OpExecutionContext, Output, asset


@asset
def unstructured_raw(context: OpExecutionContext, config: FileConfig):
    try:
        dataset_properties = define_dataset_properties(
            context, country_code=config.country_code
        )
        dataset_metadata_event = MetadataChangeProposalWrapper(
            entityUrn=config.datahub_destination_dataset_urn,
            aspect=dataset_properties,
        )
        context.log.info("EMITTING DATASET METADATA")
        datahub_emitter.emit(dataset_metadata_event)

    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)
        pass

    return Output(None)
