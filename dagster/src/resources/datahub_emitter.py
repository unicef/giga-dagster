from datetime import datetime

import datahub.emitter.mce_builder as builder
import pandas as pd
from datahub.emitter.mce_builder import make_data_platform_urn, make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
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

from dagster import OpExecutionContext, version
from src._utils.adls import get_input_filepath, get_output_filepath
from src.settings import settings


def create_domains():
    domains = ["Geospatial", "Infrastructure", "School", "Finance"]

    for domain in domains:
        domain_urn = make_domain_urn(domain)
        domain_properties_aspect = DomainPropertiesClass(name=domain, description="")

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=domain_urn,
            aspect=domain_properties_aspect,
        )

        datahub_emitter = DatahubRestEmitter(
            gms_server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )

        datahub_emitter.emit(event)

    return


def create_dataset_urn(context: OpExecutionContext, upstream: bool) -> str:
    output_filepath = get_output_filepath(context)
    input_filepath = get_input_filepath(context)

    dataset_urn_name = output_filepath.split(".")[0]  # Removes file extension
    dataset_urn_name = dataset_urn_name.replace("/", ".")  # Datahub reads '.' as folder

    upstream_urn_name = input_filepath.split(".")[0]  # Removes file extension
    upstream_urn_name = upstream_urn_name.replace(
        "/", "."
    )  # Datahub reads '.' as folder

    if upstream:
        return builder.make_dataset_urn(platform="adls", name=upstream_urn_name)
    else:
        return builder.make_dataset_urn(platform="adls", name=dataset_urn_name)


def define_dataset_properties(context: OpExecutionContext):
    step = context.asset_key.to_user_string()
    output_filepath = get_output_filepath(context)

    domain = context.get_step_execution_context().op_config["dataset_type"]
    file_size_bytes = context.get_step_execution_context().op_config["file_size_bytes"]
    metadata = context.get_step_execution_context().op_config["metadata"]

    data_format = output_filepath.split(".")[-1]
    country = output_filepath.split("/")[2].split("_")[0]
    source = output_filepath.split("_")[2]
    date_modified = output_filepath.split(".")[0].split("_")[-1]
    asset_type = "Raw Data" if step == "raw" else "Table"
    file_size_MB = file_size_bytes / 1000000  # bytes to MB

    # current date and time format: day/month/year 24-hour clock
    now = datetime.now().strftime("%d/%b/%Y %H:%M:%S")

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
        description=f"{step}",
        customProperties=custom_metadata,
    )

    return dataset_properties


def define_schema_properties(df: pd.DataFrame):
    columns = list(df.columns)
    dtypes = list(df.dtypes)
    fields = []

    for column, dtype in list(zip(columns, dtypes)):
        if dtype == "float64" or dtype == "int64":
            type_class = NumberTypeClass()
        elif dtype == "datetime64[ns]":
            type_class = DateTypeClass()
        else:
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

    return schema_properties


def set_domain(context: OpExecutionContext):
    domain = context.get_step_execution_context().op_config["dataset_type"]

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

    return domain_urn


def emit_metadata_to_datahub(context: OpExecutionContext, df: pd.DataFrame):
    # Instantiate a Datahub Rest Emitter client
    datahub_emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )

    # Set the dataset's URN
    dataset_urn = create_dataset_urn(context, upstream=False)

    # Construct Dataset metadata MCP
    dataset_properties = define_dataset_properties(context)
    dataset_metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    # Emit Dataset metadata! This is a blocking call
    context.log.info("EMITTING DATASET METADATA")
    datahub_emitter.emit(dataset_metadata_event)

    # Construct Schema metadata MCP
    schema_properties = define_schema_properties(df)
    schema_metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_properties,
    )

    # Emit Schema metadata! This is a blocking call
    context.log.info("EMITTING SCHEMA")
    datahub_emitter.emit(schema_metadata_event)

    # Instantiate a Datahub Graph client
    graph = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )

    # Create domain mutation query
    domain_urn = set_domain(context)
    domain_query = f"""
    mutation setDomain {{
        setDomain(domainUrn: "{domain_urn}", entityUrn: "{dataset_urn}")
    }}
    """

    # Emit Domain metadata! This is a blocking call
    context.log.info("EMITTING DOMAIN METADATA")
    graph.execute_graphql(query=domain_query)

    # Construct a Lineage object
    step = context.asset_key.to_user_string()
    if step != "raw":
        upstream_dataset_urn = create_dataset_urn(context, upstream=True)
        lineage_mce = builder.make_lineage_mce(
            [upstream_dataset_urn],  # Upstream URNs
            dataset_urn,  # Downstream URN
        )

        # Emit lineage metadata!
        context.log.info("EMITTING LINEAGE METADATA")
        datahub_emitter.emit_mce(lineage_mce)

    return context.log.info(
        f"Metadata of dataset {get_output_filepath(context)} has been successfully"
        " emitted to Datahub."
    )