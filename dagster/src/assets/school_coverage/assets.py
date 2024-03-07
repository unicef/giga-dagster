import time

import pandas as pd
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark import sql
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    row_level_checks,
)
from src.sensors.base import FileConfig
from src.settings import settings
from src.spark.coverage_transform_functions import (
    fb_coverage_merge,
    fb_transforms,
    itu_coverage_merge,
    itu_transforms,
)
from src.utils.adls import ADLSFileClient, get_filepath, get_output_filepath
from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub

from dagster import AssetOut, OpExecutionContext, Output, asset, multi_asset


@asset(io_manager_key="adls_pandas_io_manager")
def coverage_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
) -> pd.DataFrame:
    df = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"]
    )

    emit_metadata_to_datahub(context, df=df)
    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})


@multi_asset(
    outs={
        "coverage_dq_results": AssetOut(
            is_required=True, io_manager_key="adls_pandas_io_manager"
        ),
        "coverage_dq_summary_statistics": AssetOut(
            is_required=True, io_manager_key="adls_json_io_manager"
        ),
    }
)
def coverage_data_quality_results(
    context,
    config: FileConfig,
    coverage_raw: sql.DataFrame,
    spark: PySparkResource,
):
    filepath = context.run_tags["dagster/run_key"].split("/")[-1]
    country_code = filepath.split("_")[1]
    source = filepath.split("_")[3]

    dq_results = row_level_checks(coverage_raw, f"coverage_{source}", country_code)
    dq_summary_statistics = aggregate_report_json(
        aggregate_report_spark_df(spark.spark_session, dq_results), coverage_raw
    )

    yield Output(
        dq_results.toPandas(),
        metadata={"filepath": get_output_filepath(context, "coverage_dq_results")},
        output_name="coverage_dq_results",
    )

    yield Output(
        dq_summary_statistics,
        metadata={
            "filepath": get_output_filepath(context, "coverage_dq_summary_statistics")
        },
        output_name="coverage_dq_summary_statistics",
    )


@asset(io_manager_key="adls_delta_io_manager")
def coverage_dq_passed_rows(
    context: OpExecutionContext,
    coverage_dq_results: sql.DataFrame,
) -> sql.DataFrame:
    df_passed = coverage_dq_results
    yield Output(df_passed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def coverage_dq_failed_rows(
    context: OpExecutionContext,
    coverage_dq_results: sql.DataFrame,
) -> sql.DataFrame:
    df_failed = coverage_dq_results
    emit_metadata_to_datahub(context, df_failed)
    yield Output(df_failed, metadata={"filepath": get_output_filepath(context)})


# OUTPUT OF THIS IS A STAGING DATAFRAME COMPOSED OF INCOMING COVERAGE DATA + CURRENT SILVER COVERAGE


# SOME QUESTIONS:
# 1. What if multiple raw files
@asset(io_manager_key="adls_pandas_io_manager")
def coverage_bronze(
    context: OpExecutionContext,
    coverage_dq_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    filepath = context.run_tags["dagster/run_key"].split("/")[-1]
    source = filepath.split("_")[3]
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    silver_table_name = filepath.split("/").split("_")[1]

    silver_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'silver').split('/')[:-1]}/{silver_table_name}"

    if DeltaTable.isDeltaTable(spark.spark_session, silver_table_path):
        silver = adls_file_client.download_delta_table_as_spark_dataframe(
            silver_table_path, spark.spark_session
        )

    if source == "fb":
        df = fb_transforms(coverage_dq_passed_rows)

        if silver:
            df = fb_coverage_merge(df, silver)
    else:
        df = itu_transforms(coverage_dq_passed_rows)

        if silver:
            df = itu_coverage_merge(df, silver)

    emit_metadata_to_datahub(context, df=df)  # check if df being passed in is correct
    yield Output(df.toPandas(), metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def coverage_staging(
    context: OpExecutionContext,
    coverage_bronze: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
):
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    filepath = context.run_tags["dagster/run_key"]
    silver_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'silver').split('_')[0]}"
    staging_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'staging').split('_')[0]}"
    country_code = filepath.split("/")[-1].split("_")[1]

    # If a staging table already exists, how do we prevent merging files that were already merged?
    # {filepath: str, date_modified: str}
    files_for_review = []
    for file_data in adls_file_client.list_paths(
        f"staging/pending-review/school-{dataset_type}-data"
    ):
        if (
            file_data["is_directory"]
            or file_data["name"].split("/")[-1].split("_")[0] != country_code
        ):
            continue
        else:
            properties = adls_file_client.get_file_metadata(file_data["name"])
            date_modified = properties["metadata"]["Date_Modified"]
            context.log.info(
                f"filepath: {file_data['name']}, date_modified: {date_modified}"
            )
            files_for_review.append(
                {"filepath": file_data["name"], "date_modified": date_modified}
            )

    files_for_review.sort(
        key=lambda x: time.mktime(
            time.strptime(x["date_modified"], "%d/%m/%Y %H:%M:%S")
        )
    )

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
        staging = coverage_bronze
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

    emit_metadata_to_datahub(context, staging)
