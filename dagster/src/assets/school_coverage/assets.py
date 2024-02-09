import time

import pandas as pd
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark import sql
from src.utils.adls import ADLSFileClient, get_filepath, get_output_filepath
# from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_raw_io_manager")
def coverage_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
) -> pd.DataFrame:
    df = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"]
    )

    # emit_metadata_to_datahub(context, df=df)
    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})


@asset(
    io_manager_key="adls_delta_io_manager",
)
def coverage_data_quality_results(
    context,
    coverage_raw: sql.DataFrame,
):
    # Output is a spark dataframe (bronze + extra columns with dq results)
    # Output is a spark dataframe(?) with summary statistics (for Ger)
    # Output is a JSON with a list of checks (no results - Ger asked for)
    # Use multiassets
    yield Output(
        coverage_raw, metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_delta_io_manager")
def coverage_dq_passed_rows(
    context: OpExecutionContext,
    coverage_data_quality_results: sql.DataFrame,
) -> sql.DataFrame:
    df_passed = coverage_data_quality_results
    yield Output(df_passed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def coverage_dq_failed_rows(
    context: OpExecutionContext,
    coverage_data_quality_results: sql.DataFrame,
) -> sql.DataFrame:
    df_failed = coverage_data_quality_results
    # emit_metadata_to_datahub(context, df_failed)
    yield Output(df_failed, metadata={"filepath": get_output_filepath(context)})


# OUTPUT OF THIS IS A STAGING DATAFRAME COMPOSED OF INCOMING COVERAGE DATA + CURRENT SILVER COVERAGE


# SOME QUESTIONS:
# 1. What if multiple raw files
@asset(io_manager_key="adls_delta_io_manager")
def coverage_bronze(
    context: OpExecutionContext, coverage_dq_passed_rows: pd.DataFrame
) -> sql.DataFrame:
    # fb data - add missing columns in ITU dataset
    # special transform ni renz - use existing silver table - fb transform & ITU transform
    # output is fb + silver or ITU + silver
    # # emit_metadata_to_datahub(context, df=raw) # check if df being passed in is correct
    yield Output(
        coverage_dq_passed_rows, metadata={"filepath": get_output_filepath(context)}
    )

    # silver - 0
    # fb 0 - 1
    # fb 1 - 0
    # fb 2 - 0

    # silver + fb 0 -> 1
    # silver + fb 1 -> 0
    # silver + fb 2 -> 0


@asset(io_manager_key="adls_delta_io_manager")
def coverage_staging(
    context: OpExecutionContext,
    coverage_bronze: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    # one staging dataset for fb/itu both
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    incoming_data_filepath = context.run_tags["dagster/run_key"]
    country_code = incoming_data_filepath.split("/")[-1].split("_")[0]
    silver_table_path = get_filepath(incoming_data_filepath, dataset_type, "silver")

    # {filepath: str, date_modified: str}
    files_for_review = []
    for file_data in adls_file_client.list_paths(
        f"staging/pending-review/{dataset_type}"
    ):
        if (
            file_data["is_directory"]
            or file_data["name"].split("/")[-1].split("_")[0] != country_code
        ):
            continue
        else:
            properties = adls_file_client.get_file_metadata(file_data["name"])
            date_modified = properties["metadata"]["Date_Modified"]
            files_for_review.append(
                {"filepath": file_data["name"], "date_modified": date_modified}
            )

    files_for_review.sort(
        key=lambda x: time.mktime(
            time.strptime(x["date_modified"], "%d/%m/%Y %H:%M:%S")
        )
    )

    if DeltaTable.isDeltaTable(spark, silver_table_path):
        # Clone silver table to staging folder
        silver = adls_file_client.download_delta_table_as_spark_dataframe(
            silver_table_path, spark.spark_session
        )

        staging_table_path = get_filepath(
            incoming_data_filepath, dataset_type, "staging"
        )
        adls_file_client.upload_spark_dataframe_as_delta_table_within_dagster(
            context, silver, staging_table_path, spark.spark_session
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

            staging.alias("source").merge(
                existing_file.alias("target"),
                "source.school_id_giga = target.school_id_giga",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            context.log.info(f"Staging: table {staging}")
        staging = (
            staging.toDF()
        )  # not sure if this is needed, seems like merge is in place

    else:
        staging = coverage_bronze
        # If no existing silver table, just merge the spark dataframes
        for file_date in files_for_review:
            existing_file = adls_file_client.download_delta_table_as_spark_dataframe(
                file_date["filepath"], spark.spark_session
            )
            staging = staging.union(existing_file)

    # # # emit_metadata_to_datahub(context, staging)
    yield Output(staging, metadata={"filepath": get_output_filepath(context)})
