import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetPropertiesClass

from dagster import OpExecutionContext
from src._utils.adls import get_input_filepath, get_output_filepath
from src.settings import settings


def emit_metadata_to_datahub(context: OpExecutionContext) -> None:
    datahub_emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )

    # Construct a dataset properties object
    dataset_properties = DatasetPropertiesClass(
        description=f"{context.asset_key.to_user_string()}",
        customProperties={"governance": "ENABLED"},
    )

    # Set the dataset's URN
    dataset_urn = create_dataset_urn(context, upstream=False)

    # Construct a MetadataChangeProposalWrapper object
    metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    # Emit metadata! This is a blocking call
    context.log.info("Emitting metadata")
    datahub_emitter.emit(metadata_event)

    if context.asset_key.to_user_string() != "raw":
        upstream_dataset_urn = create_dataset_urn(context, upstream=True)

        # Construct a lineage object
        lineage_mce = builder.make_lineage_mce(
            [upstream_dataset_urn],  # Upstream URNs
            dataset_urn,  # Downstream URN
        )

        # Emit lineage metadata!
        context.log.info("Emitting lineage metadata")
        datahub_emitter.emit_mce(lineage_mce)

    context.log.info(
        f"Metadata of dataset {get_output_filepath(context)} has been successfully"
        " emitted to Datahub."
    )


def create_dataset_urn(context: OpExecutionContext, upstream: bool) -> str:
    if upstream:
        return builder.make_dataset_urn(
            platform="adls", name=get_input_filepath(context)
        )
    else:
        return builder.make_dataset_urn(
            platform="adls", name=get_output_filepath(context)
        )
