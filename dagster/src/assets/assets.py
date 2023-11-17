import datahub.emitter.mce_builder as builder
import pandas as pd
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetPropertiesClass

from dagster import OpExecutionContext, Output, asset  # AssetsDefinition
from src.resources.get_destination_file_path import get_destination_filepath
from src.settings import DATAHUB_ACCESS_TOKEN, DATAHUB_METADATA_SERVER_URL

# from dagster_ge import ge_validation_op_factory


def output_filepath(context):
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    source_path = context.get_step_execution_context().op_config["filepath"]
    step = context.asset_key.to_user_string()

    destination_filepath = get_destination_filepath(source_path, dataset_type, step)

    return destination_filepath


def emit_metadata_to_datahub(context, upstream_dataset_urn):
    rest_emitter = DatahubRestEmitter(
        gms_server=f"http://{DATAHUB_METADATA_SERVER_URL}", token=DATAHUB_ACCESS_TOKEN
    )

    # Construct a dataset properties object
    dataset_properties = DatasetPropertiesClass(
        description=f"{context.asset_key.to_user_string()}",
        customProperties={"governance": "ENABLED"},
    )

    # Set the dataset's URN
    dataset_urn = builder.make_dataset_urn(
        platform="adls", name=output_filepath(context)
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
        f"Metadata of dataset {output_filepath(context)} has been successfully emitted"
        " to Datahub."
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

    yield Output(df, metadata={"filepath": output_filepath(context)})
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

    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})
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
    context.log.info(f"data={df}")
    yield Output(
        df, metadata={"filepath": context.run_tags["dagster/run_key"]}
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
    yield Output(
        df, metadata={"filepath": context.run_tags["dagster/run_key"]}
    )  # io manager should upload this to silver/<dataset_name> as deltatable


@asset(
    io_manager_key="adls_io_manager",
)
def gold(context: OpExecutionContext, silver: pd.DataFrame) -> pd.DataFrame:
    df = silver
    df["f"] = True
    yield Output(
        df, metadata={"filepath": context.run_tags["dagster/run_key"]}
    )  # io manager should upload this to gold/master_data as deltatable


############

# @asset(
#     io_manager_key="adls_io_manager",
# )
# def bronze(context: OpExecutionContext, raw: pd.DataFrame) -> pd.DataFrame:
#     df = raw
#     df["d"] = 12345

#     emitter = DatahubRestEmitter(gms_server="http://datahub-gms:8080", extra_headers={})

#     # Test the connection
#     emitter.test_connection()

#     # Construct a dataset properties object
#     dataset_properties = DatasetPropertiesClass(
#         description="This table stored the canonical User profile",
#         customProperties={"governance": "ENABLED"},
#     )

#     # Construct a MetadataChangeProposalWrapper object.
#     metadata_event = MetadataChangeProposalWrapper(
#         entityUrn=builder.make_dataset_urn("adls", "cereals.highest_calorie_cereal"),
#         entityType="dataHubIngestionSource",
#         changeType="CREATE",
#         aspect=dataset_properties,
#     )

#     # Emit metadata! This is a blocking call
#     context.log.info("EMITTING METADATA")
#     emitter.emit(metadata_event)

#     yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})
#     # return Output(df, metadata={"filename": df.shape[0]})
#     # return (
#     #     raw_file.transform()
#     # )  # io manager should upload this to bronze/<dataset_name> as deltatable
