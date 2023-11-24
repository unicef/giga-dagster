from datetime import datetime

import datahub.emitter.mce_builder as builder
import pandas as pd
from datahub.emitter.mce_builder import make_data_platform_urn, make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    DomainPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

from dagster import OpExecutionContext, Output, asset, version  # AssetsDefinition
from src.resources._utils import get_input_filepath, get_output_filepath
from src.settings import DATAHUB_ACCESS_TOKEN, DATAHUB_METADATA_SERVER_URL

# from dagster_ge import ge_validation_op_factory


def create_domains_in_datahub():
    domains = ["Geospatial", "Infrastructure", "School", "Finance"]

    for domain in domains:
        domain_urn = make_domain_urn(domain)
        domain_properties_aspect = DomainPropertiesClass(name=domain, description="")

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityType="domain",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=domain_urn,
            aspect=domain_properties_aspect,
        )

        rest_emitter = DatahubRestEmitter(
            gms_server=f"http://{DATAHUB_METADATA_SERVER_URL}",
            token=DATAHUB_ACCESS_TOKEN,
        )

        rest_emitter.emit(event)


def emit_metadata_to_datahub(
    context: OpExecutionContext, upstream_dataset_urn, df: pd.DataFrame
):
    rest_emitter = DatahubRestEmitter(
        gms_server=f"http://{DATAHUB_METADATA_SERVER_URL}", token=DATAHUB_ACCESS_TOKEN
    )

    step = context.asset_key.to_user_string()
    output_filepath = get_output_filepath(context)

    file_size_bytes = context.get_step_execution_context().op_config["file_size_bytes"]
    metadata = context.get_step_execution_context().op_config["metadata"]

    data_format = output_filepath.split(".")[-1]
    country = output_filepath.split("/")[2].split("_")[0]
    # domain = output_filepath.split("/")[2].split("_")[1] #out-of-range daw whaaat
    domain = context.get_step_execution_context().op_config["dataset_type"]
    source = output_filepath.split("_")[2]
    date_modified = output_filepath.split(".")[0].split("_")[-1]
    asset_type = "Raw Data" if step == "raw" else "Table"
    file_size_MB = file_size_bytes / 1000000  # bytes to MB

    # datetime containing current date and time # dd/mm/YY H:M:S
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    custom_metadata = {
        "Dagster Asset Key": f"{step}",
        "Metadata Last Ingested": f"{now}",
        "Asset Type": f"{asset_type}",
        "Sensitivity Level": "",
        "Data Security Classification (UNICEF)": "",
        "PII Classification": "",
        "Date Uploaded": "",
        "Geolocation Data Source": f"{source}",
        "Data Collection Modality": "",
        "Data Collection Date": "",
        "Domain": f"{domain}",
        "Date_Modified": f"{date_modified}",
        "Source": f"{source}",
        "Data Format": f"{data_format}",
        "Data Size": f"{file_size_MB} MB",
        "Data Owner": "",
        "Data Schema": "",
        "Country": f"{country}",
        "School ID Type": "",
        "Description of file update": "",
        "Dagster version": version.__version__,
    } | metadata

    # Construct a dataset properties object
    dataset_properties = DatasetPropertiesClass(
        description=f"{context.asset_key.to_user_string()}",
        customProperties=custom_metadata,
    )

    # Set the dataset's URN
    dataset_urn = builder.make_dataset_urn(platform="adls", name=output_filepath)

    # Construct a MetadataChangeProposalWrapper object
    metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    # Emit metadata! This is a blocking call
    context.log.info("EMITTING METADATA")
    context.log.info(f"metadata: {custom_metadata}")
    rest_emitter.emit(metadata_event)

    ##### SCHEMA #####
    context.log.info(df.info)
    columns = list(df.columns)
    dtypes = list(df.dtypes)
    fields = []

    for column, dtype in list(zip(columns, dtypes)):
        if dtype == "float64" or dtype == "int64":
            type_class = NumberTypeClass()
        elif dtype == "datetime64[ns]":
            type_class = DateTypeClass()
        elif dtype == "object":
            type_class = StringTypeClass()
        else:
            context.log.info("Unknown Type")
            type_class = StringTypeClass()

        fields.append(
            SchemaFieldClass(
                fieldPath=f"{column}",
                type=SchemaFieldDataTypeClass(type_class),
                nativeDataType=f"{dtype}",  # use this to provide the type of the field in the source system's vernacular
            )
        )

    schema_properties = SchemaMetadataClass(
        schemaName="placeholder",  # not used
        platform=make_data_platform_urn("adls"),  # important <- platform must be an urn
        version=0,  # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
        hash="",  # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
        platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),
        fields=fields,
    )

    # Construct a MetadataChangeProposalWrapper object
    schema_metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_properties,
    )

    # Emit metadata! This is a blocking call
    context.log.info("EMITTING SCHEMA")
    context.log.info(f"schema: {schema_properties}")
    rest_emitter.emit(schema_metadata_event)

    ##### DOMAIN #####
    graph = DataHubGraph(
        DatahubClientConfig(
            server=f"http://{DATAHUB_METADATA_SERVER_URL}", token=DATAHUB_ACCESS_TOKEN
        )
    )

    if "school" in domain:
        domain_urn = make_domain_urn("School")
    elif "geospatial" in domain:
        domain_urn = make_domain_urn("Geospatial")
    elif "fin" in domain:
        domain_urn = make_domain_urn("Finance")
    elif "infra" in domain:
        domain_urn = make_domain_urn("Infrastructure")
    else:
        context.log.info("UNKNOWN DOMAIN")
        domain_urn = make_domain_urn("UNKOWN DOMAIN")

    # Query multiple aspects from entity
    query = f"""
    mutation setDomain {{
        setDomain(domainUrn: {domain_urn}, entityUrn: {dataset_urn})
    }}
    """
    # Emit domain metadata!
    context.log.info("EMITTING DOMAIN")
    graph.execute_graphql(query=query)

    ##### LINEAGE #####
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
        f"Metadata of dataset {output_filepath} has been successfully"
        " emitted to Datahub."
    )


@asset(io_manager_key="adls_io_manager", required_resource_keys={"adls_file_client"})
def raw(context: OpExecutionContext) -> pd.DataFrame:
    df = context.resources.adls_file_client.download_from_adls(
        context.run_tags["dagster/run_key"]
    )
    context.log.info(df.head())

    # Create domains in Datahub
    # Emit metadata! This is a blocking call
    context.log.info("CREATING DOMAINS IN DATAHUB")
    create_domains_in_datahub()

    # Emit metadata of dataset to Datahub
    emit_metadata_to_datahub(context, upstream_dataset_urn="", df=df)

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
    emit_metadata_to_datahub(context, upstream_dataset_urn=raw_dataset_urn, df=df)

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
    emit_metadata_to_datahub(context, upstream_dataset_urn=bronze_dataset_urn, df=df)

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
        context, upstream_dataset_urn=staging_pending_review_dataset_urn, df=df
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
    emit_metadata_to_datahub(
        context, upstream_dataset_urn=staging_approved_dataset_urn, df=df
    )

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
    emit_metadata_to_datahub(context, upstream_dataset_urn=silver_dataset_urn, df=df)

    yield Output(
        df, metadata={"filepath": get_output_filepath(context)}
    )  # io manager should upload this to gold/master_data as deltatable
