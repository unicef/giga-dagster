import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass

from dagster import OpExecutionContext
from src._utils.adls import get_input_filepath, get_output_filepath


def emit_metadata_to_datahub(context: OpExecutionContext) -> None:
    # Construct a dataset properties object
    dataset_properties = DatasetPropertiesClass(
        description=f"{context.asset_key.to_user_string()}",
        customProperties={"governance": "ENABLED"},
    )

    # Set the dataset's URN
    dataset_urn = create_dataset_urn(context)

    # Construct a MetadataChangeProposalWrapper object
    metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    # Emit metadata! This is a blocking call
    context.log.info("Emitting metadata")
    context.resources.datahub_emitter.emit(metadata_event)

    if context.asset_key.to_user_string() != "raw":
        upstream_dataset_urn = create_dataset_urn(context)

        # Construct a lineage object
        lineage_mce = builder.make_lineage_mce(
            [upstream_dataset_urn],  # Upstream URNs
            dataset_urn,  # Downstream URN
        )

        # Emit lineage metadata!
        context.log.info("Emitting lineage metadata")
        context.resources.datahub_emitter.emit_mce(lineage_mce)

    context.log.info(
        f"Metadata of dataset {get_output_filepath(context)} has been successfully"
        " emitted to Datahub."
    )


def create_dataset_urn(context: OpExecutionContext) -> str:
    return builder.make_dataset_urn(platform="adls", name=get_input_filepath(context))
