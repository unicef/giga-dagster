import time

import pandas as pd
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark import sql
from src.settings import settings
from src.utils.adls import ADLSFileClient, get_filepath, get_output_filepath
from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub

from dagster import AssetOut, OpExecutionContext, Output, asset, multi_asset


@asset(io_manager_key="adls_pandas_io_manager")
def geolocation_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
) -> pd.DataFrame:
    df = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"]
    )
    # IN: pandas df, OUT: csv file
    emit_metadata_to_datahub(context, df=df)
    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})


@asset(io_manager_key="adls_pandas_io_manager")  # this is wrong
def geolocation_bronze(
    context: OpExecutionContext, geolocation_raw: sql.DataFrame
) -> pd.DataFrame:
    # IN: spark datafame, OUT: csv file
    # Transform columns added here, all column renaming done here
    # Output should be stored as a spark dataframe
    emit_metadata_to_datahub(context, df=geolocation_raw)
    yield Output(
        geolocation_raw.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@multi_asset(
    outs={
        "geolocation_dq_results": AssetOut(
            is_required=True, io_manager_key="adls_pandas_io_manager"
        ),
        "geolocation_dq_summary_statistics": AssetOut(
            is_required=True, io_manager_key="adls_pandas_io_manager"
        ),
        "geolocation_dq_checks": AssetOut(
            is_required=True, io_manager_key="adls_json_io_manager"
        ),
    }
)
def geolocation_data_quality_results(
    context,
    geolocation_bronze: sql.DataFrame,
):
    # Output is a spark dataframe (bronze + extra columns with dq results)
    # Output is a spark dataframe(?) with summary statistics (for Ger)
    # Output is a JSON with a list of checks (no results - Ger asked for)
    yield Output(
        geolocation_bronze.toPandas(),
        metadata={"filepath": get_output_filepath(context)},
        output_name="geolocation_dq_results",
    )

    yield Output(
        geolocation_bronze.toPandas(),
        metadata={"filepath": get_output_filepath(context)},
        output_name="geolocation_dq_summary_statistics",
    )

    yield Output(
        geolocation_bronze,
        metadata={"filepath": get_output_filepath(context)},
        output_name="geolocation_dq_checks",
    )


@asset(io_manager_key="adls_delta_io_manager")
def geolocation_dq_passed_rows(
    context: OpExecutionContext,
    geolocation_dq_results: sql.DataFrame,
) -> sql.DataFrame:
    df_passed = geolocation_dq_results
    df_passed.toPandas().loc[3, "school_id_giga"] = "ABCDEFGHIJKLM"
    context.log.info(f"df_passed: {df_passed}")
    yield Output(df_passed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def geolocation_dq_failed_rows(
    context: OpExecutionContext,
    geolocation_dq_results: sql.DataFrame,
) -> sql.DataFrame:
    df_failed = geolocation_dq_results
    emit_metadata_to_datahub(context, df_failed)
    yield Output(df_failed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def geolocation_staging(
    context: OpExecutionContext,
    geolocation_bronze: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
):
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    filepath = context.run_tags["dagster/run_key"]
    silver_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'silver').split('_')[0]}"
    staging_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'staging').split('_')[0]}"
    country_code = filepath.split("/")[-1].split("_")[0]

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

            context.log.info(f"Staging: table {staging}")

        staging.execute()

    else:
        staging = geolocation_bronze
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

    # emit_metadata_to_datahub(context, staging)
