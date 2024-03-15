from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from sqlalchemy import select
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_failed_rows,
    dq_passed_rows,
    row_level_checks,
)
from src.schemas.file_upload import FileUploadConfig
from src.sensors.base import FileConfig
from src.settings import settings
from src.spark.transform_functions import (
    add_missing_columns,
    create_giga_school_id,
)
from src.utils.adls import (
    ADLSFileClient,
    deconstruct_filename_components,
    get_filepath,
    get_output_filepath,
    validate_filename,
)
from src.utils.datahub.create_validation_tab import EmitDatasetAssertionResults
from src.utils.datahub.emit_dataset_metadata import (
    create_dataset_urn,
    emit_metadata_to_datahub,
)
from src.utils.db import get_db_context
from src.utils.metadata import get_output_metadata
from src.utils.pandas import pandas_loader
from src.utils.schema import get_schema_columns

from dagster import AssetOut, OpExecutionContext, Output, asset, multi_asset


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
        dataset_urn=config.datahub_dataset_urn,
    )
    yield Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key="adls_pandas_io_manager")  # this is wrong
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
        file_upload = FileUploadConfig.from_orm(file_upload)

    column_mapping = {
        k: v for k, v in file_upload.column_to_schema_mapping.items() if v is not None
    }

    with BytesIO(geolocation_raw) as buffer:
        buffer.seek(0)
        pdf = pandas_loader(buffer, config.filepath)

    schema_columns = get_schema_columns(spark.spark_session, config.metastore_schema)
    df = s.createDataFrame(pdf)
    df = df.withColumnsRenamed(column_mapping)
    df = add_missing_columns(df, schema_columns)
    df = create_giga_school_id(df)
    df = df.withColumn("connectivity_govt_ingestion_timestamp", current_timestamp())

    emit_metadata_to_datahub(
        context,
        df=df,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_dataset_urn,
    )
    yield Output(df.toPandas(), metadata=get_output_metadata(config))


@multi_asset(
    outs={
        "geolocation_dq_results": AssetOut(
            is_required=True, io_manager_key="adls_pandas_io_manager"
        ),
        "geolocation_dq_summary_statistics": AssetOut(
            is_required=True, io_manager_key="adls_json_io_manager"
        ),
    }
)
def geolocation_data_quality_results(
    context,
    config: FileConfig,
    geolocation_bronze: sql.DataFrame,
    spark: PySparkResource,
):
    country_code = config.filename_components.country_code
    dq_results = row_level_checks(geolocation_bronze, "geolocation", country_code)
    dq_summary_statistics = aggregate_report_json(
        aggregate_report_spark_df(spark.spark_session, dq_results), geolocation_bronze
    )

    yield Output(
        dq_results.toPandas(),
        metadata=get_output_metadata(
            config, get_output_filepath(context, "geolocation_dq_results")
        ),
        output_name="geolocation_dq_results",
    )

    context.log.info("EMITTING ASSERTIONS TO DATAHUB")
    dataset_urn = create_dataset_urn(context, is_upstream=False)
    emit_assertions = EmitDatasetAssertionResults(
        dataset_urn=dataset_urn, dq_summary_statistics=dq_summary_statistics
    )
    emit_assertions()
    context.log.info("SUCCESS! DATASET VALIDATION TAB CREATED IN DATAHUB")

    yield Output(
        dq_summary_statistics,
        metadata=get_output_metadata(
            config, get_output_filepath(context, "geolocation_dq_summary_statistics")
        ),
        output_name="geolocation_dq_summary_statistics",
    )


@asset(io_manager_key="adls_pandas_io_manager")
def geolocation_dq_passed_rows(
    context: OpExecutionContext,
    geolocation_dq_results: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
    df_passed = dq_passed_rows(geolocation_dq_results, config.dataset_type)
    emit_metadata_to_datahub(
        context,
        df_passed,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_dataset_urn,
    )
    yield Output(df_passed.toPandas(), metadata=get_output_metadata(config))


@asset(io_manager_key="adls_pandas_io_manager")
def geolocation_dq_failed_rows(
    context: OpExecutionContext,
    geolocation_dq_results: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
    df_failed = dq_failed_rows(geolocation_dq_results, config.dataset_type)
    emit_metadata_to_datahub(
        context,
        df_failed,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_dataset_urn,
    )
    yield Output(df_failed.toPandas(), metadata=get_output_metadata(config))


@asset(io_manager_key="adls_delta_io_manager")
def geolocation_staging(
    context: OpExecutionContext,
    geolocation_dq_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
):
    dataset_type = config.dataset_type
    silver_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(config.filepath, dataset_type, 'silver').split('_')[0]}"
    staging_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(config.filepath, dataset_type, 'staging').split('_')[0]}"

    # {filepath: str, date_modified: str}
    files_for_review = []
    for file_data in adls_file_client.list_paths_generator(
        f"staging/pending-review/school-{dataset_type}-data"
    ):
        pending_filename_components = deconstruct_filename_components(file_data.name)
        if file_data.is_directory or (
            pending_filename_components.country_code
            != config.filename_components.country_code
        ):
            continue

        properties = adls_file_client.get_file_metadata(file_data["name"])
        date_modified = properties.last_modified
        context.log.info(f"filepath: {file_data.name}, date_modified: {date_modified}")
        files_for_review.append(
            {"filepath": file_data.name, "date_modified": date_modified}
        )

    files_for_review.sort(key=lambda x: x.last_modified)

    context.log.info(f"files_for_review: {files_for_review}")

    # If silver table exists and no staging table exists, clone it to staging
    # If silver table exists and staging table exists, merge files for review to existing staging table
    # If silver table does not exist, merge files for review into one spark dataframe
    if DeltaTable.isDeltaTable(spark.spark_session, silver_table_path):
        if not DeltaTable.isDeltaTable(spark.spark_session, staging_table_path):
            # Clone silver table to staging folder
            silver = adls_file_client.download_delta_table_as_spark_dataframe(
                silver_table_path, spark.spark_session
            )

            adls_file_client.upload_spark_dataframe_as_delta_table(
                silver,
                staging_table_path,
                context.op_config["dataset_type"],
                spark.spark_session,
            )

        # Load new table (silver clone in staging) as a deltatable
        staging = adls_file_client.download_delta_table_as_delta_table(
            staging_table_path, spark.spark_session
        )

        # Merge each pending file for the same country
        for file_info in files_for_review:
            existing_file = adls_file_client.download_delta_table_as_spark_dataframe(
                file_info["filepath"], spark.spark_session
            )

            staging = (
                staging.alias("source")
                .merge(
                    existing_file.alias("target"),
                    "source.school_id_giga = target.school_id_giga",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
            )

            adls_file_client.rename_file(
                file_info["filepath"], f"{file_info['filepath']}/merged-files"
            )

            context.log.info(f"Staging: table {staging}")

        staging.execute()

    else:
        staging = geolocation_dq_passed_rows
        # If no existing silver table, just merge the spark dataframes
        for file_date in files_for_review:
            existing_file = adls_file_client.download_delta_table_as_spark_dataframe(
                file_date["filepath"], spark.spark_session
            )
            staging = staging.union(existing_file)

        adls_file_client.upload_spark_dataframe_as_delta_table(
            staging,
            staging_table_path,
            context.op_config["dataset_type"],
            spark.spark_session,
        )

    emit_metadata_to_datahub(
        context,
        df=staging,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_dataset_urn,
    )
