from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import SparkSession
from sqlalchemy import select
from src.constants import DataTier
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.resources import ResourceKey
from src.schemas.file_upload import FileUploadConfig
from src.spark.transform_functions import (
    column_mapping_rename,
    create_bronze_layer_columns,
)
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.datahub.create_validation_tab import EmitDatasetAssertionResults
from src.utils.datahub.emit_dataset_metadata import (
    create_dataset_urn,
    emit_metadata_to_datahub,
)
from src.utils.db import get_db_context
from src.utils.delta import (
    build_deduped_merge_query,
    create_delta_table,
    create_schema,
    execute_query_with_error_handler,
)
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.pandas import pandas_loader
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_primary_key,
    get_schema_columns,
)
from src.utils.spark import compute_row_hash, transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def geolocation_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> Output[bytes]:
    raw = adls_file_client.download_raw(config.filepath)
    emit_metadata_to_datahub(
        context,
        df=raw,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )
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
    emit_metadata_to_datahub(
        context,
        df=df,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )
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

    context.log.info("EMITTING ASSERTIONS TO DATAHUB")
    dataset_urn = create_dataset_urn(context, is_upstream=False)
    emit_assertions = EmitDatasetAssertionResults(
        dataset_urn=dataset_urn,
        dq_summary_statistics=dq_summary_statistics,
        context=context,
    )
    emit_assertions()
    context.log.info("SUCCESS! DATASET VALIDATION TAB CREATED IN DATAHUB")

    return Output(dq_summary_statistics, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def geolocation_dq_passed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
) -> Output[pd.DataFrame]:
    df_passed = dq_split_passed_rows(
        geolocation_data_quality_results, config.dataset_type
    )
    emit_metadata_to_datahub(
        context,
        df_passed,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )
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
) -> Output[pd.DataFrame]:
    df_failed = dq_split_failed_rows(
        geolocation_data_quality_results, config.dataset_type
    )
    emit_metadata_to_datahub(
        context,
        df_failed,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )
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
    s: SparkSession = spark.spark_session
    schema_name = config.metastore_schema
    country_code = config.filename_components.country_code
    schema_columns = get_schema_columns(s, schema_name)
    primary_key = get_primary_key(s, schema_name)
    silver_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.SILVER
    )
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )

    # If silver table exists and no staging table exists, clone it to staging
    # If silver table exists and staging table exists, merge files for review to existing staging table
    # If silver table does not exist, merge files for review into one spark dataframe
    if s.catalog.tableExists(silver_table_name):
        if not s.catalog.tableExists(staging_table_name):
            # Clone silver table to staging
            silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
            create_schema(s, staging_tier_schema_name)
            create_delta_table(
                s,
                staging_tier_schema_name,
                country_code,
                schema_columns,
                context,
                if_not_exists=True,
            )
            silver.write.format("delta").mode("append").saveAsTable(staging_table_name)

        # Load new table (silver clone in staging) as a deltatable
        staging_dt = DeltaTable.forName(s, staging_table_name)
        staging_detail = staging_dt.detail().toDF()
        staging_last_modified = (
            staging_detail.select("lastModified").first().lastModified
        )
        staging = staging_dt.alias("staging").toDF()

        files_for_review = sorted(
            [
                p
                for p in adls_file_client.list_paths(
                    str(config.filepath_object.parent), recursive=False
                )
                if p.last_modified >= staging_last_modified
            ],
            key=lambda p: p.last_modified,
        )
        context.log.info(f"{len(files_for_review)=}")

        # Merge each pending file for the same country
        for file_info in files_for_review:
            existing_file = adls_file_client.download_csv_as_spark_dataframe(
                file_info.name, spark.spark_session
            )
            existing_file = transform_types(existing_file, schema_name, context)
            existing_file = compute_row_hash(existing_file)
            staging_dt = DeltaTable.forName(s, staging_table_name)
            update_columns = [c.name for c in schema_columns if c.name != primary_key]
            query = build_deduped_merge_query(
                staging_dt, existing_file, primary_key, update_columns
            )

            if query is not None:
                execute_query_with_error_handler(
                    s, query, staging_tier_schema_name, country_code, context
                )

            staging = staging_dt.toDF()
    else:
        staging = geolocation_dq_passed_rows
        staging = transform_types(staging, schema_name, context)
        files_for_review = sorted(
            [
                p
                for p in adls_file_client.list_paths(
                    str(config.filepath_object.parent), recursive=False
                )
                if p.name != config.filepath
            ],
            key=lambda p: p.last_modified,
        )
        context.log.info(f"{len(files_for_review)=}")

        create_schema(s, staging_tier_schema_name)
        create_delta_table(
            s,
            staging_tier_schema_name,
            country_code,
            schema_columns,
            context,
            if_not_exists=True,
        )
        # If no existing silver table, just merge the spark dataframes
        for file_info in files_for_review:
            existing_file = adls_file_client.download_csv_as_spark_dataframe(
                file_info.name, spark.spark_session
            )
            existing_file = transform_types(existing_file, schema_name, context)
            context.log.info(f"{existing_file.count()=}")
            staging = staging.union(existing_file)
            context.log.info(f"{staging.count()=}")

        staging = compute_row_hash(staging)
        context.log.info(f"Full {staging.count()=}")
        staging.write.format("delta").mode("append").saveAsTable(staging_table_name)

    emit_metadata_to_datahub(
        context,
        df=staging,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )
    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(staging),
        },
    )
