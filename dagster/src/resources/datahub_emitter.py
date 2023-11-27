from datetime import datetime

import datahub.emitter.mce_builder as builder
import pandas as pd
from datahub.emitter.mce_builder import (  # make_container_urn,
    make_data_platform_urn,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# from datahub.emitter.mcp_builder import (
#     # FolderKey,
#     # add_dataset_to_container,
#     # gen_containers,
# )
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (  # ChangeTypeClass,; ContainerPropertiesClass,
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
from src.resources._utils import get_output_filepath
from src.settings import DATAHUB_ACCESS_TOKEN, DATAHUB_METADATA_SERVER_URL


def create_domains():
    domains = ["Geospatial", "Infrastructure", "School", "Finance"]

    for domain in domains:
        domain_urn = make_domain_urn(domain)
        domain_properties_aspect = DomainPropertiesClass(name=domain, description="")

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=domain_urn,
            aspect=domain_properties_aspect,
        )

        rest_emitter = DatahubRestEmitter(
            gms_server=f"http://{DATAHUB_METADATA_SERVER_URL}",
            token=DATAHUB_ACCESS_TOKEN,
        )

        rest_emitter.emit(event)


# def create_dataset_containers():
#     containers = ['raw', 'bronze', 'staging', 'silver', 'gold']

#     for container in containers:
#         container_urn = make_container_urn(FolderKey(
#             platform='adls', env='PROD', folder_abs_path=container))
#         container_properties_aspect = ContainerPropertiesClass(name=container, description="")

#         # Construct a MetadataChangeProposalWrapper object
#         container_event = MetadataChangeProposalWrapper(
#             entityUrn=container_urn,
#             aspect=container_properties_aspect
#         )

#         rest_emitter = DatahubRestEmitter(
#             gms_server=f"http://{DATAHUB_METADATA_SERVER_URL}",
#             token=DATAHUB_ACCESS_TOKEN,
#         )

#         rest_emitter.emit(container_event)

# def create_dataset_containers():

#     containers = ['raw', 'bronze', 'staging', 'silver', 'gold']

#     for container in containers:
#         container_event = gen_containers(
#             container_key=FolderKey(platform='adls', env='PROD', folder_abs_path=container),
#             name=container,
#             sub_types=[],
#         )

#         rest_emitter = DatahubRestEmitter(
#             gms_server=f"http://{DATAHUB_METADATA_SERVER_URL}",
#             token=DATAHUB_ACCESS_TOKEN,
#         )

#         # Emit metadata! This is a blocking call
#         rest_emitter.emit(container_event)


def emit_metadata_to_datahub(
    context: OpExecutionContext, upstream_dataset_urn, df: pd.DataFrame
):
    rest_emitter = DatahubRestEmitter(
        gms_server=f"http://{DATAHUB_METADATA_SERVER_URL}", token=DATAHUB_ACCESS_TOKEN
    )

    ##### CREATE DATASET WITH CUSTOM PROPERTIES METADATA #####

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

    dataset_urn_name = output_filepath.split(".")[0]  # Removes file extension
    dataset_urn_name = dataset_urn_name.replace("/", ".")  # Datahub reads '.' as folder

    # Set the dataset's URN
    dataset_urn = builder.make_dataset_urn(platform="adls", name=dataset_urn_name)

    # Construct a MetadataChangeProposalWrapper object
    metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    # Emit metadata! This is a blocking call
    context.log.info("EMITTING METADATA")
    context.log.info(f"metadata: {custom_metadata}")
    rest_emitter.emit(metadata_event)

    # ##### ADD DATASET TO A CONTAINER #####
    # add_dataset_to_container_event = add_dataset_to_container(
    #     container_key=FolderKey(platform='adls', env='PROD', folder_abs_path=step),
    #     dataset_urn=dataset_urn
    # )

    # # Emit metadata! This is a blocking call
    # context.log.info(f"ADDING DATASET TO {step} CONTAINER")
    # rest_emitter.emit(add_dataset_to_container_event)

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

    context.log.info(domain_urn)
    context.log.info(dataset_urn)

    # Query multiple aspects from entity
    query = f"""
    mutation setDomain {{
        setDomain(domainUrn: "{domain_urn}", entityUrn: "{dataset_urn}")
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
