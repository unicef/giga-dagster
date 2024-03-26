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
from src.utils.delta import create_delta_table
from src.utils.filename import deconstruct_filename_components, validate_filename
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.pandas import pandas_loader
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_schema_columns,
)

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def geolocation_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    validate_filename(config.filepath)
    raw = adls_file_client.download_raw(config.filepath)
    emit_metadata_to_datahub(
        context,
        df=raw,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )
    yield Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key="adls_pandas_io_manager")
def geolocation_bronze(
    context: OpExecutionContext,
    geolocation_raw: bytes,
    config: FileConfig,
    spark: PySparkResource,
) -> pd.DataFrame:
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
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "column_mapping": column_mapping,
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key="adls_pandas_io_manager")
def geolocation_data_quality_results(
    context: OpExecutionContext,
    config: FileConfig,
    geolocation_bronze: sql.DataFrame,
) -> pd.DataFrame:
    country_code = config.filename_components.country_code
    dq_results = row_level_checks(
        geolocation_bronze, "geolocation", country_code, context
    )

    dq_pandas = dq_results.toPandas()
    yield Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(dq_pandas),
        },
    )


@asset(io_manager_key="adls_json_io_manager")
def geolocation_data_quality_results_summary(
    context: OpExecutionContext,
    geolocation_bronze: sql.DataFrame,
    geolocation_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> dict | list[dict]:
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

    yield Output(dq_summary_statistics, metadata=get_output_metadata(config))


@asset(io_manager_key="adls_pandas_io_manager")
def geolocation_dq_passed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
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
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key="adls_pandas_io_manager")
def geolocation_dq_failed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
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
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key="adls_delta_io_manager")
def geolocation_staging(
    context: OpExecutionContext,
    geolocation_dq_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
):
    s: SparkSession = spark.spark_session
    dataset_type = config.dataset_type
    schema_name = config.metastore_schema
    country_code = config.filename_components.country_code
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

    files_for_review = sorted(
        [
            p
            for p in adls_file_client.list_paths(
                f"staging/pending-review/school-{dataset_type}-data"
            )
            if p.is_directory
            or deconstruct_filename_components(p.name).country_code
            != config.filename_components.country_code
        ],
        key=lambda x: x.date_modified,
    )

    context.log.info(f"files_for_review: {files_for_review}")

    # If silver table exists and no staging table exists, clone it to staging
    # If silver table exists and staging table exists, merge files for review to existing staging table
    # If silver table does not exist, merge files for review into one spark dataframe
    if s.catalog.tableExists(silver_table_name):
        if not s.catalog.tableExists(staging_table_name):
            # Clone silver table to staging
            silver = DeltaTable.forName(silver_table_name).alias("silver").toDF()
            create_delta_table(
                s, staging_tier_schema_name, country_code, silver.schema, context
            )
            silver.write.format("delta").mode("append").saveAsTable(staging_table_name)

        # Load new table (silver clone in staging) as a deltatable
        staging = DeltaTable.forName(s, staging_table_name).alias("staging").toDF()

        # Merge each pending file for the same country
        for file_info in files_for_review:
            existing_file = adls_file_client.download_delta_table_as_spark_dataframe(
                file_info["filepath"], spark.spark_session
            )

            (
                staging.alias("staging")
                .merge(
                    existing_file.alias("updates"),
                    "staging.school_id_giga = updates.school_id_giga",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            adls_file_client.rename_file(
                file_info["filepath"], f"{file_info['filepath']}/merged-files"
            )
            context.log.info(f"Staging: table {staging}")

    else:
        staging = geolocation_dq_passed_rows
        # If no existing silver table, just merge the spark dataframes
        for file_date in files_for_review:
            existing_file = adls_file_client.download_delta_table_as_spark_dataframe(
                file_date["filepath"], spark.spark_session
            )
            staging = staging.union(existing_file)

        adls_file_client.upload_spark_dataframe_as_delta_table(
            staging, schema_name, staging_table_name, s
        )

    emit_metadata_to_datahub(
        context,
        df=staging,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )
