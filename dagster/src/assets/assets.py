import datahub.emitter.mce_builder as builder
import pandas as pd
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetPropertiesClass

from dagster import OpExecutionContext, Output, asset  # AssetsDefinition
from src.resources._utils import get_input_filepath, get_output_filepath
from src.settings import settings

# from dagster_ge import ge_validation_op_factory


def emit_metadata_to_datahub(context: OpExecutionContext, upstream_dataset_urn):
    rest_emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )

    # Construct a dataset properties object
    dataset_properties = DatasetPropertiesClass(
        description=f"{context.asset_key.to_user_string()}",
        customProperties={"governance": "ENABLED"},
    )

    # Set the dataset's URN
    dataset_urn = builder.make_dataset_urn(
        platform="adls", name=get_output_filepath(context)
    )

    # Construct a MetadataChangeProposalWrapper object
    metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    # Emit metadata! This is a blocking call
    context.log.info("EMITTING METADATA")
    rest_emitter.emit(metadata_event)

    step = context.asset_key.to_user_string()

    if step != "raw":
        # Construct a lineage object
        lineage_mce = builder.make_lineage_mce(
            [upstream_dataset_urn],  # Upstream URNs
            dataset_urn,  # Downstream URN
        )

        # Emit lineage metadata!
        context.log.info("EMITTING LINEAGE METADATA")
        rest_emitter.emit_mce(lineage_mce)

    return context.log.info(
        f"Metadata of dataset {get_output_filepath(context)} has been successfully"
        " emitted to Datahub."
    )


@asset(io_manager_key="adls_io_manager", required_resource_keys={"adls_file_client"})
def raw(context: OpExecutionContext) -> pd.DataFrame:
    df = context.resources.adls_file_client.download_from_adls(
        context.run_tags["dagster/run_key"]
    )
    context.log.info(f"data={df}")

    # Emit metadata of dataset to Datahub
    emit_metadata_to_datahub(context, upstream_dataset_urn="")

    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})
    # return df  # io manager should upload this to raw bucket as csv


@asset(
    io_manager_key="adls_io_manager",
)
def bronze(context: OpExecutionContext, raw: pd.DataFrame) -> pd.DataFrame:
    df = raw

    raw_dataset_urn = builder.make_dataset_urn(
        platform="adls", name=context.run_tags["dagster/run_key"]
    )

    # Emit metadata of dataset to Datahub
    emit_metadata_to_datahub(context, upstream_dataset_urn=raw_dataset_urn)

    yield Output(df, metadata={"filepath": get_output_filepath(context)})
    # return Output(df, metadata={"filename": df.shape[0]})
    # return (
    #     raw_file.transform()
    # )  # io manager should upload this to bronze/<dataset_name> as deltatable


# expectation_suite_op = ge_validation_op_factory(
#     name="raw_file_expectations",
#     datasource_name="raw_file",
#     suite_name="data-quality-checks",
#     validation_operator_name="action_list_operator",
# )

# expectation_suite_asset = AssetsDefinition.from_op(
#     expectation_suite_op,
#     keys_by_input_name={"dataset": AssetKey("bronze")},
# )  # not sure yet if we can return dfs from this. runs on dfs but uploads as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def dq_passed_rows(context: OpExecutionContext, bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze

    context.log.info(get_input_filepath(context, upstream_step="bronze"))

    bronze_dataset_urn = builder.make_dataset_urn(
        platform="adls", name=get_input_filepath(context, upstream_step="bronze")
    )

    # Emit metadata of dataset to Datahub
    emit_metadata_to_datahub(context, upstream_dataset_urn=bronze_dataset_urn)

    yield Output(df, metadata={"filepath": get_output_filepath(context)})
    # return (
    #     expectations_suite_asset.passed_rows()
    # )  # io manager should upload this to staging/pending-review/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def dq_failed_rows(context: OpExecutionContext, bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze
    df["doodoo"] = "doodoo"
    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})
    # return (
    #     expectations_suite_asset.failed_rows()
    # )  # io manager should upload this to archive/gx_tests_failed as deltatable


@asset(io_manager_key="adls_io_manager", required_resource_keys={"adls_file_client"})
def manual_review_passed_rows(context: OpExecutionContext) -> pd.DataFrame:
    df = context.resources.adls_file_client.download_from_adls(
        context.run_tags["dagster/run_key"]
    )

    # Emit metadata of dataset to Datahub
    staging_pending_review_dataset_urn = builder.make_dataset_urn(
        platform="adls",
        name=get_input_filepath(context, upstream_step="staging/pending-review"),
    )
    emit_metadata_to_datahub(
        context, upstream_dataset_urn=staging_pending_review_dataset_urn
    )

    context.log.info(f"data={df}")
    yield Output(
        df, metadata={"filepath": get_output_filepath(context)}
    )  # io manager should upload this to staging/approved/<dataset_name> as deltatable


@asset(io_manager_key="adls_io_manager", required_resource_keys={"adls_file_client"})
def manual_review_failed_rows(context: OpExecutionContext) -> pd.DataFrame:
    df = context.resources.adls_file_client.download_from_adls(
        context.run_tags["dagster/run_key"]
    )
    context.log.info(f"data={df}")
    yield Output(
        df, metadata={"filepath": context.run_tags["dagster/run_key"]}
    )  # io manager should upload this to archive/manual_review_rejected as deltatable


########## unsure how manual review will go yet/how datasets will be moved to the correct folders so the assets are probably wrong


@asset(
    io_manager_key="adls_io_manager",
)
def silver(context: OpExecutionContext, manual_review_passed_rows) -> pd.DataFrame:
    df = manual_review_passed_rows
    df["e"] = False

    # Emit metadata of dataset to Datahub
    staging_approved_dataset_urn = builder.make_dataset_urn(
        platform="adls",
        name=get_input_filepath(context, upstream_step="staging/approved"),
    )
    emit_metadata_to_datahub(context, upstream_dataset_urn=staging_approved_dataset_urn)

    yield Output(
        df, metadata={"filepath": get_output_filepath(context)}
    )  # io manager should upload this to silver/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def gold(context: OpExecutionContext, silver: pd.DataFrame) -> pd.DataFrame:
    df = silver
    df["f"] = True

    # Emit metadata of dataset to Datahub
    silver_dataset_urn = builder.make_dataset_urn(
        platform="adls",
        name=get_input_filepath(context, upstream_step="silver"),
    )
    emit_metadata_to_datahub(context, upstream_dataset_urn=silver_dataset_urn)

    yield Output(
        df, metadata={"filepath": get_output_filepath(context)}
    )  # io manager should upload this to gold/master_data as deltatable
