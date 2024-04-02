from io import BytesIO

import pandas as pd
import sentry_sdk
from dagster_pyspark import PySparkResource
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import SparkSession
from sqlalchemy import select
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.internal.common_assets.staging import staging_step
from src.resources import ResourceKey
from src.schemas.file_upload import FileUploadConfig
from src.spark.transform_functions import (
    column_mapping_rename,
    create_bronze_layer_columns,
)
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.datahub.builders import build_dataset_urn
from src.utils.datahub.create_validation_tab import EmitDatasetAssertionResults
from src.utils.datahub.emit_dataset_metadata import (
    emit_metadata_to_datahub,
)
from src.utils.db import get_db_context
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.pandas import pandas_loader
from src.utils.schema import (
    get_schema_columns,
    get_schema_columns_datahub,
)
from src.utils.sentry import log_op_context

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def geolocation_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> Output[bytes]:
    raw = adls_file_client.download_raw(config.filepath)

    try:
        emit_metadata_to_datahub(
            context,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)
    return Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def geolocation_bronze(
    context: OpExecutionContext,
    geolocation_raw: bytes,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session

    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id)
        )
        if file_upload is None:
            raise FileNotFoundError(
                f"Database entry for FileUpload with id `{config.filename_components.id}` was not found"
            )

        file_upload = FileUploadConfig.from_orm(file_upload)

    with BytesIO(geolocation_raw) as buffer:
        buffer.seek(0)
        pdf = pandas_loader(buffer, config.filepath)

    schema_columns = get_schema_columns(spark.spark_session, config.metastore_schema)

    df = s.createDataFrame(pdf)
    df, column_mapping = column_mapping_rename(df, file_upload.column_to_schema_mapping)
    df = create_bronze_layer_columns(df, schema_columns)
    config.metadata.update({"column_mapping": column_mapping})
    try:
        emit_metadata_to_datahub(
            context,
            schema_reference=df,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    df_pandas = df.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "column_mapping": column_mapping,
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def geolocation_data_quality_results(
    context: OpExecutionContext,
    config: FileConfig,
    geolocation_bronze: sql.DataFrame,
) -> Output[pd.DataFrame]:
    try:
        emit_metadata_to_datahub(
            context,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    country_code = config.filename_components.country_code
    dq_results = row_level_checks(
        geolocation_bronze, "geolocation", country_code, context
    )

    dq_pandas = dq_results.toPandas()
    return Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(dq_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
def geolocation_data_quality_results_summary(
    context: OpExecutionContext,
    geolocation_bronze: sql.DataFrame,
    geolocation_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[dict]:
    dq_summary_statistics = aggregate_report_json(
        aggregate_report_spark_df(
            spark.spark_session, geolocation_data_quality_results
        ),
        geolocation_bronze,
    )

    try:
        config = FileConfig(**context.get_step_execution_context().op_config)
        dq_target_dataset_urn = build_dataset_urn(filepath=config.dq_target_filepath)

        context.log.info("EMITTING ASSERTIONS TO DATAHUB...")
        emit_assertions = EmitDatasetAssertionResults(
            dq_summary_statistics=dq_summary_statistics,
            context=context,
            dataset_urn=dq_target_dataset_urn,
        )
        emit_assertions()
    except Exception as error:
        context.log.error(f"Assertion Run ERROR: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    try:
        emit_metadata_to_datahub(
            context,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    return Output(dq_summary_statistics, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def geolocation_dq_passed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    df_passed = dq_split_passed_rows(
        geolocation_data_quality_results, config.dataset_type
    )

    try:
        schema_reference = get_schema_columns_datahub(
            spark.spark_session, config.metastore_schema
        )
        emit_metadata_to_datahub(
            context,
            schema_reference=schema_reference,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    df_pandas = df_passed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def geolocation_dq_failed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    df_failed = dq_split_failed_rows(
        geolocation_data_quality_results, config.dataset_type
    )

    try:
        schema_reference = get_schema_columns_datahub(
            spark.spark_session, config.metastore_schema
        )
        emit_metadata_to_datahub(
            context,
            schema_reference=schema_reference,
            df_failed=df_failed,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    df_pandas = df_failed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def geolocation_staging(
    context: OpExecutionContext,
    geolocation_dq_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[None]:
    staging = staging_step(
        context,
        config,
        adls_file_client,
        spark.spark_session,
        upstream_df=geolocation_dq_passed_rows,
    )
    try:
        schema_reference = get_schema_columns_datahub(
            spark.spark_session, config.metastore_schema
        )
        emit_metadata_to_datahub(
            context,
            schema_reference=schema_reference,
            country_code=config.filename_components.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(staging),
        },
    )
