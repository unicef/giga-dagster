import sentry_sdk
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emitter,
    define_dataset_properties,
)
from src.utils.metadata import get_output_metadata
from src.utils.op_config import FileConfig
from src.utils.sentry import log_op_context

from dagster import OpExecutionContext, Output, asset


@asset
def unstructured_raw(context: OpExecutionContext, config: FileConfig):
    country_code = None if config.country_code == "N/A" else config.country_code

    try:
        dataset_properties = define_dataset_properties(
            context, country_code=country_code
        )
        dataset_metadata_event = MetadataChangeProposalWrapper(
            entityUrn=config.datahub_destination_dataset_urn,
            aspect=dataset_properties,
        )
        context.log.info("EMITTING DATASET METADATA")
        context.log.info(dataset_metadata_event)
        datahub_emitter.emit(dataset_metadata_event)
        context.log.info(
            f"Metadata has been successfully emitted to Datahub with dataset URN {config.datahub_destination_dataset_urn}.",
        )

    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)
        pass

    return Output(None, metadata={**get_output_metadata(config)})


@asset
def generalized_unstructured_raw(context: OpExecutionContext, config: FileConfig):
    try:
        dataset_properties = define_dataset_properties(
            context, country_code=config.country_code
        )
        dataset_metadata_event = MetadataChangeProposalWrapper(
            entityUrn=config.datahub_destination_dataset_urn,
            aspect=dataset_properties,
        )
        context.log.info("EMITTING DATASET METADATA")
        context.log.info(dataset_metadata_event)
        datahub_emitter.emit(dataset_metadata_event)
        context.log.info(
            f"Metadata has been successfully emitted to Datahub with dataset URN {config.datahub_destination_dataset_urn}.",
        )
        return Output(None, metadata={**get_output_metadata(config)})
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)
        return Output(None)
