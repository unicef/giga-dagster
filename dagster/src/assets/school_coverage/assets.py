from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from icecream import ic
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
from src.schemas.file_upload import FileUploadConfig
from src.spark.coverage_transform_functions import (
    fb_coverage_merge,
    fb_transforms,
    itu_coverage_merge,
    itu_transforms,
)
from src.spark.transform_functions import (
    column_mapping_rename,
)
from src.utils.adls import ADLSFileClient
from src.utils.datahub.create_validation_tab import EmitDatasetAssertionResults
from src.utils.datahub.emit_dataset_metadata import (
    create_dataset_urn,
    emit_metadata_to_datahub,
)
from src.utils.db import get_db_context
from src.utils.filename import deconstruct_filename_components, validate_filename
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.pandas import pandas_loader

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def coverage_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    validate_filename(config.filepath)
    df = adls_file_client.download_raw(config.filepath)

    # # for testing only START - will be moved to io manager
    # filepath = context.run_tags["dagster/run_key"].split("/")[-1]
    # country_code = filepath.split("_")[1]
    # platform = builder.make_data_platform_urn("adlsGen2")
    # dataset_urn = builder.make_dataset_urn(
    #     platform=platform,
    #     env=settings.ADLS_ENVIRONMENT,
    #     name=filepath.split(".")[0].replace("/", "."),
    # )
    # emit_metadata_to_datahub(context, df, country_code, dataset_urn)
    # # for testing only END - will be moved to io manager

    yield Output(df, metadata=get_output_metadata(config))


@asset(io_manager_key="adls_pandas_io_manager")
def coverage_data_quality_results(
    context,
    config: FileConfig,
    coverage_raw: bytes,
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

    with BytesIO(coverage_raw) as buffer:
        buffer.seek(0)
        pdf = pandas_loader(buffer, config.filepath)

    source = config.filename_components.source

    df_raw = s.createDataFrame(pdf)
    df, column_mapping = column_mapping_rename(
        df_raw, file_upload.column_to_schema_mapping
    )
    dq_results = row_level_checks(
        df,
        f"coverage_{source}",
        config.filename_components.country_code,
        context,
    )

    config.metadata.update({"column_mapping": column_mapping})
    emit_metadata_to_datahub(
        context,
        dq_results,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )
    dq_pandas = dq_results.toPandas()
    yield Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "column_mapping": column_mapping,
            "preview": get_table_preview(dq_pandas),
        },
    )


@asset(io_manager_key="adls_json_io_manager")
def coverage_data_quality_results_summary(
    context,
    config: FileConfig,
    coverage_raw: bytes,
    coverage_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
) -> dict | list[dict]:
    s: SparkSession = spark.spark_session

    with BytesIO(coverage_raw) as buffer:
        buffer.seek(0)
        pdf = pandas_loader(buffer, config.filepath)

    df_raw = s.createDataFrame(pdf)
    dq_summary_statistics = aggregate_report_json(
        aggregate_report_spark_df(spark.spark_session, coverage_data_quality_results),
        df_raw,
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
def coverage_dq_passed_rows(
    context: OpExecutionContext,
    coverage_data_quality_results: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
    df_passed = dq_split_passed_rows(coverage_data_quality_results, config.dataset_type)
    emit_metadata_to_datahub(
        context,
        df_passed,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
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
def coverage_dq_failed_rows(
    context: OpExecutionContext,
    coverage_data_quality_results: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
    df_failed = dq_split_failed_rows(coverage_data_quality_results, config.dataset_type)
    emit_metadata_to_datahub(
        context,
        df_failed,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    df_pandas = df_failed.toPandas()
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key="adls_pandas_io_manager")
def coverage_bronze(
    context: OpExecutionContext,
    coverage_dq_passed_rows: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    s: SparkSession = spark.spark_session
    source = ic(config.filename_components.source)
    silver_table_name = config.filename_components.country_code.lower()
    full_silver_table_name = f"{config.metastore_schema}.{silver_table_name}"

    if source == "fb":
        df = fb_transforms(coverage_dq_passed_rows)
    else:  # source == "itu"
        df = itu_transforms(coverage_dq_passed_rows)

    if s.catalog.tableExists(full_silver_table_name):
        silver = DeltaTable.forName(spark.spark_session, full_silver_table_name).toDF()
        if source == "fb":
            df = fb_coverage_merge(df, silver)
        else:  # source == "itu"
            df = itu_coverage_merge(df, silver)

    emit_metadata_to_datahub(
        context,
        df,
        config.filename_components.country_code,
        config.datahub_destination_dataset_urn,
    )
    df_pandas = df.toPandas()
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key="adls_delta_io_manager")
def coverage_staging(
    context: OpExecutionContext,
    coverage_bronze: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
):
    s: SparkSession = spark.spark_session

    dataset_type = config.dataset_type
    filepath = config.filepath
    metastore_schema = config.metastore_schema
    country_code = config.filename_components.country_code
    silver_table_name = f"{metastore_schema}_silver.{country_code}"
    staging_table_name = f"{metastore_schema}_staging.{country_code}"
    country_code = filepath.split("/")[-1].split("_")[1]

    files_for_review = []
    for file_data in adls_file_client.list_paths_generator(
        f"staging/pending-review/school-{dataset_type}", recursive=False
    ):
        file_data_components = deconstruct_filename_components(file_data.name)
        if file_data.is_directory or file_data_components.country_code != country_code:
            continue

        properties = adls_file_client.get_file_metadata(file_data.name)
        date_modified = properties.metadata.get("Date_Modified")
        context.log.info(f"filepath: {file_data.name}, date_modified: {date_modified}")
        files_for_review.append(
            {"filepath": file_data.name, "date_modified": date_modified}
        )

    files_for_review = sorted(files_for_review, key=lambda x: x["date_modified"])
    context.log.info(f"files_for_review: {files_for_review}")

    # If silver table exists and no staging table exists, clone it to staging
    # If silver table exists and staging table exists, merge files for review to existing staging table
    # If silver table does not exist, merge files for review into one spark dataframe
    if s.catalog.tableExists(silver_table_name):
        if not s.catalog.tableExists(staging_table_name):
            # Clone silver table to staging folder
            silver = DeltaTable.forName(s, silver_table_name).toDF()
            (
                DeltaTable.createOrReplace(s)
                .tableName(staging_table_name)
                .addColumns(silver.schema)
                .property("delta.enableChangeDataFeed", "true")
                .execute()
            )
            (
                DeltaTable.forName(s, staging_table_name)
                .merge(
                    silver.alias("silver"),
                    "silver.school_id_giga = staging.school_id_giga",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

        staging = DeltaTable.forName(s, staging_table_name)

        # Merge each pending file for the same country
        for file_info in files_for_review:
            existing_file = adls_file_client.download_csv_as_pandas_dataframe(
                file_info["filepath"]
            )
            existing_df = s.createDataFrame(existing_file)

            (
                staging.alias("staging")
                .merge(
                    existing_df.alias("updates"),
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
        staging = coverage_bronze
        # If no existing silver table, just merge the spark dataframes
        for file_date in files_for_review:
            existing_file = adls_file_client.download_csv_as_pandas_dataframe(
                file_date["filepath"]
            )
            existing_df = s.createDataFrame(existing_file)
            staging = staging.union(existing_df)

        (
            DeltaTable.forName(s, staging_table_name)
            .alias("staging")
            .merge(
                staging.alias("updates"),
                "staging.school_id_giga = updates.school_id_giga",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
