import os.path
from datetime import datetime

import country_converter as cc
import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import make_data_platform_urn, make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
)
from pyspark import sql
from src.constants import constants
from src.settings import settings
from src.utils.adls import get_output_filepath
from src.utils.op_config import FileConfig

from dagster import OpExecutionContext, version


def identify_country_name(country_code: str) -> str:
    coco = cc.CountryConverter()
    country_name = list(
        coco.data.iloc[coco.data["ISO3"][coco.data["ISO3"].isin([country_code])].index][
            "name_short"
        ]
    )[0]
    return country_name


def create_dataset_urn(
    context: OpExecutionContext, is_upstream: bool, output_name: str = None
) -> str:
    platform = builder.make_data_platform_urn("adlsGen2")
    config = FileConfig(**context.get_step_execution_context().op_config)

    # TODO: Handle multiple upstreams
    if is_upstream:
        upstream_urn_name = config.datahub_source_dataset_urn
        context.log.info(f"{upstream_urn_name=}")
        return builder.make_dataset_urn(
            platform=platform, name=upstream_urn_name, env=settings.ADLS_ENVIRONMENT
        )
    else:
        dataset_urn_name = config.datahub_destination_dataset_urn
        context.log.info(f"{dataset_urn_name=}")
        return builder.make_dataset_urn(platform=platform, name=dataset_urn_name)


def define_dataset_properties(context: OpExecutionContext, country_code: str):
    step = context.asset_key.to_user_string()
    output_filepath = get_output_filepath(context)
    config = FileConfig(**context.get_step_execution_context().op_config)

    domain = config.dataset_type
    file_size_bytes = config.file_size_bytes
    metadata = config.metadata

    data_format = os.path.splitext(output_filepath)[1].lstrip(".")
    country_name = identify_country_name(country_code=country_code)
    file_size_MB = file_size_bytes / (2 ** (10 * 2))  # bytes to MB
    run_tags = str.join(", ", [f"{k}={v}" for k, v in context.run_tags.items()])

    run_stats = context.instance.get_run_stats(context.run_id)
    start_time = datetime.fromtimestamp(run_stats.start_time).isoformat()

    custom_metadata: dict[str, str] = {
        "Dagster Asset Key": step,
        "Dagster Version": version.__version__,
        "Dagster Run ID": context.run_id,
        "Dagster Is Re-Execution": str(context.run.is_resume_retry),
        "Dagster Parent Run ID": str(context.run.parent_run_id),
        "Dagster Root Run ID": str(context.run.root_run_id),
        "Dagster Run Tags": run_tags,
        "Dagster Op Name": context.op_def.name,
        "Dagster Job Name": context.job_def.name,
        "Dagster Run Created Timestamp": start_time,
        "Dagster Sensor Name": context.run_tags.get("dagster/sensor_name"),
        "Code Version": settings.COMMIT_SHA,
        "Metadata Last Ingested": datetime.now().isoformat(),
        "Domain": domain,
        "Data Format": data_format,
        "Data Size": f"{file_size_MB} MB",
        "Country": country_name,
    }

    formatted_metadata = {}
    for k, v in metadata.items():
        if k == "column_mapping":
            v = ", ".join([f"{k} -> {v}" for k, v in v.items()])

        friendly_name = k.replace("_", " ").title()
        formatted_metadata[friendly_name] = v

    dataset_properties = DatasetPropertiesClass(
        description=step,
        customProperties={**formatted_metadata, **custom_metadata},
    )

    return dataset_properties


def define_schema_properties(
    schema_reference: list[tuple] | sql.DataFrame, df_failed: None | sql.DataFrame
):
    fields = []

    {
        "Double": NumberTypeClass(),
        "Float": NumberTypeClass()["Double", "Float", "Integer", "Long"],
    }

    if isinstance(schema_reference, sql.DataFrame):
        for field in schema_reference.schema.fields:
            for v in constants.TYPE_MAPPINGS.dict().values():
                if field.dataType == v["pyspark"]:
                    type_class = v["datahub"]
                else:
                    type_class = NullTypeClass

                fields.append(
                    SchemaFieldClass(
                        fieldPath=f"{field.name}",
                        type=SchemaFieldDataTypeClass(type_class()),
                        nativeDataType=f"{field.dataType}",  # use this to provide the type of the field in the source system's vernacular
                    )
                )

    else:
        for column, type_class in schema_reference:
            fields.append(
                SchemaFieldClass(
                    fieldPath=f"{column}",
                    type=SchemaFieldDataTypeClass(type_class),
                    nativeDataType=f"{type_class}",  # use this to provide the type of the field in the source system's vernacular
                )
            )

        if df_failed is not None:
            for column in df_failed.columns:
                if column.startswith("dq"):
                    fields.append(
                        SchemaFieldClass(
                            fieldPath=f"{column}",
                            type=SchemaFieldDataTypeClass(NumberTypeClass()),
                            nativeDataType="int",  # use this to provide the type of the field in the source system's vernacular
                        )
                    )

    schema_properties = SchemaMetadataClass(
        schemaName="placeholder",  # not used
        platform=make_data_platform_urn(
            "adlsGen2"
        ),  # important <- platform must be a urn
        version=0,  # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
        hash="",  # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
        platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),
        fields=fields,
    )

    return schema_properties


def set_domain(context: OpExecutionContext):
    config = FileConfig(**context.get_step_execution_context().op_config)
    domain = config.dataset_type

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
    context: OpExecutionContext,
    country_code: str,
    dataset_urn: str,
    schema_reference: sql.DataFrame | list[tuple] = None,
    df_failed: sql.DataFrame = None,
):
    datahub_emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
    )

    dataset_properties = define_dataset_properties(context, country_code=country_code)
    dataset_metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    context.log.info("EMITTING DATASET METADATA")
    datahub_emitter.emit(dataset_metadata_event)

    if schema_reference is not None:
        schema_properties = define_schema_properties(
            schema_reference, df_failed=df_failed
        )
        schema_metadata_event = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_properties,
        )

        context.log.info("EMITTING SCHEMA")
        context.log.info(schema_metadata_event)
        datahub_emitter.emit(schema_metadata_event)

    datahub_graph_client = DataHubGraph(
        DatahubClientConfig(
            server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )
    )

    domain_urn = set_domain(context)
    domain_query = f"""
    mutation setDomain {{
        setDomain(domainUrn: "{domain_urn}", entityUrn: "{dataset_urn}")
    }}
    """

    context.log.info(domain_query)
    context.log.info("EMITTING DOMAIN METADATA")
    datahub_graph_client.execute_graphql(query=domain_query)

    output_filepath = get_output_filepath(context)
    country_name = identify_country_name(country_code=country_code)
    tag_query = set_tag_mutation_query(
        country_name=country_name, dataset_urn=dataset_urn
    )
    context.log.info(tag_query)

    context.log.info("EMITTING TAG METADATA")
    datahub_graph_client.execute_graphql(query=tag_query)

    step = context.asset_key.to_user_string()
    if "raw" not in step:
        upstream_dataset_urn = create_dataset_urn(context, is_upstream=True)
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
