from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import datahub.emitter.mce_builder as builder
import sentry_sdk
from dagster_pyspark import PySparkResource
from datahub.emitter.mce_builder import make_data_platform_urn, make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
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

from dagster import OpExecutionContext, version
from src.constants import constants
from src.settings import settings
from src.utils.datahub.column_metadata import add_column_metadata, get_column_licenses
from src.utils.datahub.emit_lineage import emit_lineage
from src.utils.datahub.graphql import datahub_graph_client
from src.utils.datahub.identify_country_name import identify_country_name
from src.utils.datahub.update_policies import update_policy_for_group
from src.utils.op_config import FileConfig
from src.utils.schema import get_schema_column_descriptions
from src.utils.sentry import log_op_context


def create_dataset_urn(
    context: OpExecutionContext,
    *,
    is_upstream: bool,
    platform_id: str = "adlsGen2",
) -> str:
    platform = builder.make_data_platform_urn(platform_id)
    config = FileConfig(**context.get_step_execution_context().op_config)

    # TODO: Handle multiple upstreams
    if is_upstream:
        upstream_urn_name = config.datahub_source_dataset_urn
        context.log.info(f"{upstream_urn_name=}")
        return builder.make_dataset_urn(
            platform=platform,
            name=upstream_urn_name,
            env=settings.DATAHUB_ENVIRONMENT,
        )

    dataset_urn_name = config.datahub_destination_dataset_urn
    context.log.info(f"{dataset_urn_name=}")
    return builder.make_dataset_urn(platform=platform, name=dataset_urn_name)


def define_dataset_properties(
    context: OpExecutionContext, country_code: str | None
) -> DatasetPropertiesClass:
    step = context.asset_key.to_user_string()
    config = FileConfig(**context.get_step_execution_context().op_config)

    file_size_bytes = config.file_size_bytes
    metadata = config.metadata

    output_filepath = config.destination_filepath
    output_file_extension = Path(output_filepath).suffix.lstrip(".")
    data_format = "deltaTable" if output_file_extension == "" else output_file_extension

    country_name = None
    if country_code:
        country_name = identify_country_name(country_code=country_code)

    file_size_MB = file_size_bytes / (2 ** (10 * 2))  # noqa:N806  bytes to MB
    run_tags = str.join(", ", [f"{k}={v}" for k, v in context.run_tags.items()])
    run_stats = context.instance.get_run_stats(context.run_id)
    start_time = datetime.fromtimestamp(
        run_stats.start_time, tz=ZoneInfo("UTC")
    ).isoformat()

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
        "Metadata Last Ingested": datetime.now(tz=ZoneInfo("UTC")).isoformat(),
        "Data Format": data_format,
    }

    if country_name:
        custom_metadata["Country"] = country_name

    if data_format != "deltaTable":
        custom_metadata["Data Size"] = f"{file_size_MB:.2f} MB"

    if config.domain:
        domain = config.domain
        custom_metadata = custom_metadata | {"Domain": domain}

    formatted_metadata = {}
    for k, v in metadata.items():
        if k == "column_mapping":
            v = ", ".join([f"{k} -> {v}" for k, v in v.items()])  # noqa:PLW2901

        friendly_name = k.replace("_", " ").title()
        formatted_metadata[friendly_name] = v

    return DatasetPropertiesClass(
        customProperties={**formatted_metadata, **custom_metadata}
    )


def define_schema_properties(
    schema_reference: list[tuple] | sql.DataFrame,
    df_failed: None | sql.DataFrame,
) -> SchemaMetadataClass:
    fields = []

    if isinstance(schema_reference, sql.DataFrame):
        for field in schema_reference.schema.fields:
            is_field_type_found = False
            for v in constants.TYPE_MAPPINGS.dict().values():
                if field.dataType == v["pyspark"]():
                    type_class = v["datahub"]()
                    native_type = str(v["native"])
                    is_field_type_found = True
                    break
            if not is_field_type_found:
                type_class = NullTypeClass()
                native_type = str(None)

            fields.append(
                SchemaFieldClass(
                    fieldPath=field.name,
                    type=SchemaFieldDataTypeClass(type_class),
                    nativeDataType=native_type,
                ),
            )

    else:
        for column, type_class in schema_reference:
            fields.append(
                SchemaFieldClass(
                    fieldPath=column,
                    type=SchemaFieldDataTypeClass(type_class),
                    nativeDataType=f"{type_class}",
                ),
            )

        if df_failed is not None:
            for column in df_failed.columns:
                if column.startswith("dq"):
                    fields.append(  # noqa:PERF401
                        SchemaFieldClass(
                            fieldPath=column,
                            type=SchemaFieldDataTypeClass(NumberTypeClass()),
                            nativeDataType="int",
                        ),
                    )

    return SchemaMetadataClass(
        schemaName="placeholder",  # not used
        platform=make_data_platform_urn("adlsGen2"),
        version=0,  # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
        hash="",  # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=fields,
    )


def set_domain(context: OpExecutionContext) -> str:
    config = FileConfig(**context.get_step_execution_context().op_config)
    domain = config.domain
    context.log.info(f"Domain: {domain}")
    return make_domain_urn(domain=domain.capitalize())


def add_tag_query(tag_key: str, dataset_urn: str) -> str:
    return f"""
        mutation {{
            addTag(input:{{
                tagUrn: "urn:li:tag:{tag_key}",
                resourceUrn: "{dataset_urn}"
            }})
        }}
    """


datahub_emitter = DatahubRestEmitter(
    gms_server=settings.DATAHUB_METADATA_SERVER_URL,
    token=settings.DATAHUB_ACCESS_TOKEN,
    retry_max_times=5,
    retry_status_codes=[
        403,
        429,
        500,
        502,
        503,
        504,
    ],
)


def emit_metadata_to_datahub(
    context: OpExecutionContext,
    country_code: str,
    dataset_urn: str,
    schema_reference: None | sql.DataFrame | list[tuple] = None,
    df_failed: None | sql.DataFrame = None,
) -> None:
    dataset_properties = define_dataset_properties(context, country_code=country_code)
    dataset_metadata_event = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties,
    )

    context.log.info("EMITTING DATASET METADATA")
    datahub_emitter.emit(dataset_metadata_event)

    if schema_reference is not None:
        schema_properties = define_schema_properties(
            schema_reference,
            df_failed=df_failed,
        )
        schema_metadata_event = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_properties,
        )

        context.log.info("EMITTING SCHEMA")
        context.log.info(schema_metadata_event)
        datahub_emitter.emit(schema_metadata_event)

    domain_urn = set_domain(context)
    domain_query = f"""
    mutation setDomain {{
        setDomain(domainUrn: "{domain_urn}", entityUrn: "{dataset_urn}")
    }}
    """

    context.log.info(domain_query)
    context.log.info("EMITTING DOMAIN METADATA")
    datahub_graph_client.execute_graphql(query=domain_query)

    country_name = identify_country_name(country_code=country_code)
    tag_query = add_tag_query(tag_key=country_name, dataset_urn=dataset_urn)
    context.log.info(tag_query)

    context.log.info("EMITTING TAG METADATA")
    datahub_graph_client.execute_graphql(query=tag_query)

    context.log.info(
        f"Metadata has been successfully emitted to Datahub with dataset URN {dataset_urn}.",
    )


def datahub_emit_metadata_with_exception_catcher(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource = None,
    schema_reference: None | sql.DataFrame | list[tuple] = None,
    df_failed: None | sql.DataFrame = None,
) -> None:
    try:
        emit_metadata_to_datahub(
            context=context,
            country_code=config.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
            schema_reference=schema_reference,
            df_failed=df_failed,
        )
        try:
            emit_lineage(context=context)
        except Exception as error:
            context.log.warning(f"Cannot emit lineage: {error}")
            expected_error_info = (
                "Expected error if QoS since we do not ingest QoS upstream datasets."
            )
            context.log.warning(expected_error_info)
            log_op_context(context)
            sentry_sdk.capture_exception(error=error)
            sentry_sdk.capture_message(expected_error_info)
            pass
        if schema_reference is not None:
            context.log.info("EMITTING COLUMN METADATA...")
            column_descriptions = get_schema_column_descriptions(
                spark.spark_session,
                config.metastore_schema,
            )
            try:
                column_licenses = get_column_licenses(config=config)
                context.log.info(column_licenses)
            except Exception as e:
                context.log.warning(f"Cannot retrieve licenses: {e}")
                context.log.warning(
                    "No column licenses defined for datasets after staging."
                )
                column_licenses = None
                pass
            add_column_metadata(
                dataset_urn=config.datahub_destination_dataset_urn,
                column_licenses=column_licenses,
                column_descriptions=column_descriptions,
                context=context,
            )
        else:
            context.log.info("NO SCHEMA TO EMIT for this step.")

        try:
            context.log.info("DATAHUB UPDATE POLICIES...")
            update_policy_for_group(config=config, context=context)
        except Exception as error:
            context.log.error(f"Error on Datahub Update Policies: {error}")
            log_op_context(context)
            sentry_sdk.capture_exception(error=error)
            pass
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)
        pass


if __name__ == "__main__":
    output_filepath = "gold/BEN"
    data_format = Path(output_filepath).suffix.lstrip(".")
    print(data_format == "")
