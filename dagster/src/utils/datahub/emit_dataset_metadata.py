from datetime import datetime

import country_converter as cc
import datahub.emitter.mce_builder as builder
import pandas as pd
from datahub.emitter.mce_builder import make_data_platform_urn, make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DateTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from src.settings import settings

# from src.utils.adls import get_input_filepath
from dagster import OutputContext, version


def identify_country_name(country_code):
    coco = cc.CountryConverter()
    country_name = list(
        coco.data.iloc[coco.data["ISO3"][coco.data["ISO3"].isin([country_code])].index][
            "name_short"
        ]
    )[0]
    return country_name


def create_dataset_urn(
    output_filepath: str, input_filepath: str, upstream: bool
) -> str:
    if upstream:
        upstream_urn_name = input_filepath.split(".")[0]  # Removes file extension
        upstream_urn_name = upstream_urn_name.replace(
            "/", "."
        )  # Datahub reads '.' as folder
        return builder.make_dataset_urn(
            platform="adls", name=upstream_urn_name, env=settings.ADLS_ENVIRONMENT
        )
    else:
        dataset_urn_name = output_filepath.split(".")[0]  # Removes file extension
        dataset_urn_name = dataset_urn_name.replace(
            "/", "."
        )  # Datahub reads '.' as folder
        return builder.make_dataset_urn(
            platform="adls", name=dataset_urn_name, env=settings.ADLS_ENVIRONMENT
        )


def define_dataset_properties(context: OutputContext, output_filepath, input_filepath):
    step = context.asset_key.to_user_string()

    domain = context.step_context.op_config["dataset_type"]
    file_size_bytes = context.step_context.op_config["file_size_bytes"]
    metadata = context.step_context.op_config["metadata"]

    data_format = output_filepath.split(".")[-1]
    country_code = input_filepath.split("/")[-1].split("_")[0]

    context.log.info(country_code)

    country_name = identify_country_name(country_code=country_code)
    source = output_filepath.split("_")[2]
    date_modified = output_filepath.split(".")[0].split("_")[-1]
    asset_type = "Raw Data" if "raw" in step else "Table"
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
        "Country": f"{country_name}",
        "School ID Type": "",
        "Description of file update": "",
        "Dagster version": version.__version__,
    } | metadata

    dataset_properties = DatasetPropertiesClass(
        description=f"{step}",
        customProperties=custom_metadata,
    )

    return dataset_properties


def define_schema_properties(df: pd.DataFrame):
    columns = list(df.columns)
    dtypes = list(df.dtypes)
    fields = []

    for column, dtype in list(zip(columns, dtypes, strict=False)):
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


def set_domain(context: OutputContext):
    domain = context.step_context.op_config["dataset_type"]

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
        domain_urn = make_domain_urn("UNKNOWN DOMAIN")

    return domain_urn


def set_tag_mutation_query(country_name, dataset_urn):
    query = f"""
        mutation {{
            addTag(input:{{
                tagUrn: "urn:li:tag:{country_name}",
                resourceUrn: "{dataset_urn}"
            }})
        }}
    """

    return query


def emit_metadata_to_datahub(
    context: OutputContext, output_filepath: str, input_filepath: str, df: pd.DataFrame
):
    datahub_emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )

    dataset_urn = create_dataset_urn(output_filepath, input_filepath, upstream=False)

    dataset_properties = define_dataset_properties(
        context, output_filepath, input_filepath
    )
    dataset_metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    context.log.info("EMITTING DATASET METADATA")
    datahub_emitter.emit(dataset_metadata_event)

    schema_properties = define_schema_properties(df)
    schema_metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_properties,
    )

    context.log.info("EMITTING SCHEMA")
    datahub_emitter.emit(schema_metadata_event)

    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )

    domain_urn = set_domain(context)
    if "UNKNOWN" not in domain_urn:
        domain_query = f"""
        mutation setDomain {{
            setDomain(domainUrn: "{domain_urn}", entityUrn: "{dataset_urn}")
        }}
        """

        context.log.info(domain_query)
        context.log.info("EMITTING DOMAIN METADATA")
        datahub_graph_client.execute_graphql(query=domain_query)

    country_code = input_filepath.split("/")[-1].split("_")[0]
    context.log.info(f"Country code: {country_code}")

    country_name = identify_country_name(country_code=country_code)
    tag_query = set_tag_mutation_query(
        country_name=country_name, dataset_urn=dataset_urn
    )
    context.log.info(tag_query)

    context.log.info("EMITTING TAG METADATA")
    datahub_graph_client.execute_graphql(query=tag_query)

    step = context.asset_key.to_user_string()
    if "raw" not in step:
        upstream_dataset_urn = create_dataset_urn(
            output_filepath, input_filepath, upstream=True
        )
        lineage_mce = builder.make_lineage_mce(
            [upstream_dataset_urn],  # Upstream URNs
            dataset_urn,  # Downstream URN
        )

        context.log.info("EMITTING LINEAGE METADATA")
        datahub_emitter.emit_mce(lineage_mce)

    return context.log.info(
        f"Metadata of dataset {output_filepath} has been successfully"
        " emitted to Datahub."
    )
