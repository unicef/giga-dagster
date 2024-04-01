import pandas as pd
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark import sql
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    row_level_checks,
)
from src.schemas.qos import SchoolListConfig
from src.settings import settings
from src.spark.transform_functions import create_bronze_layer_columns
from src.utils.adls import (
    ADLSFileClient,
    get_filepath,
    get_output_filepath,
)
from src.utils.apis import query_API_data
from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub
from src.utils.db import get_db_context
from src.utils.op_config import FileConfig

from dagster import AssetOut, OpExecutionContext, Output, asset, multi_asset


@asset(io_manager_key="adls_pandas_io_manager")
def qos_school_list_raw(
    context: OpExecutionContext, config: SchoolListConfig
) -> pd.DataFrame:
    row_data = config

    with get_db_context() as database_session:
        df = pd.DataFrame.from_records(
            query_API_data(context, database_session, row_data)
        )
    emit_metadata_to_datahub(context, df=df)
    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})


@asset(io_manager_key="adls_pandas_io_manager")  # this is wrong
def qos_school_list_bronze(
    context: OpExecutionContext,
    qos_school_list_raw: sql.DataFrame,
) -> pd.DataFrame:
    ## @RENZ NEED TO ADD TRANSFORM TO CONVERT COLUMNS TO SCHEMA COLUMNS
    df = create_bronze_layer_columns(qos_school_list_raw)
    emit_metadata_to_datahub(context, df=qos_school_list_raw)
    yield Output(df.toPandas(), metadata={"filepath": get_output_filepath(context)})


@multi_asset(
    outs={
        "qos_school_list_dq_results": AssetOut(
            is_required=True, io_manager_key="adls_pandas_io_manager"
        ),
        "qos_school_list_dq_summary_statistics": AssetOut(
            is_required=True, io_manager_key="adls_json_io_manager"
        ),
    }
)
def qos_school_list_data_quality_results(
    context,
    config: FileConfig,
    qos_school_list_bronze: sql.DataFrame,
    spark: PySparkResource,
):
    country_code = context.run_tags["dagster/run_key"].split("/")[-1].split("_")[1]
    dq_results = row_level_checks(qos_school_list_bronze, "geolocation", country_code)
    dq_summary_statistics = aggregate_report_json(
        aggregate_report_spark_df(spark.spark_session, dq_results),
        qos_school_list_bronze,
    )

    yield Output(
        dq_results.toPandas(),
        metadata={
            "filepath": get_output_filepath(context, "qos_school_list_dq_results")
        },
        output_name="qos_school_list_dq_results",
    )

    yield Output(
        dq_summary_statistics,
        metadata={
            "filepath": get_output_filepath(
                context, "qos_school_list_dq_summary_statistics"
            )
        },
        output_name="qos_school_list_dq_summary_statistics",
    )


@asset(io_manager_key="adls_pandas_io_manager")
def qos_school_list_dq_passed_rows(
    context: OpExecutionContext,
    qos_school_list_dq_results: sql.DataFrame,
) -> sql.DataFrame:
    df_passed = qos_school_list_dq_results
    emit_metadata_to_datahub(context, df_passed)
    yield Output(
        df_passed.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_pandas_io_manager")
def qos_school_list_dq_failed_rows(
    context: OpExecutionContext,
    qos_school_list_dq_results: sql.DataFrame,
) -> sql.DataFrame:
    df_failed = qos_school_list_dq_results
    emit_metadata_to_datahub(context, df_failed)
    yield Output(
        df_failed.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_delta_io_manager")
def qos_school_list_staging(
    context: OpExecutionContext,
    qos_school_list_dq_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: SchoolListConfig,
):
    dataset_type = config["dataset_type"]
    filepath = context.run_tags["dagster/run_key"]
    silver_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'silver').split('_')[0]}"
    staging_table_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'staging').split('_')[0]}"
    country_code = filepath.split("/")[-1].split("_")[1]

    # {filepath: str, date_modified: str}
    files_for_review = []
    for file_data in adls_file_client.qos_school_list_paths(
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

    files_for_review.sort(key=lambda x: x["date_modified"])

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
                context.op_config["dataset_type"],
                staging_table_path,
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
        staging = qos_school_list_dq_passed_rows
        # If no existing silver table, just merge the spark dataframes
        for file_date in files_for_review:
            existing_file = adls_file_client.download_delta_table_as_spark_dataframe(
                file_date["filepath"], spark.spark_session
            )
            staging = staging.union(existing_file)

        adls_file_client.upload_spark_dataframe_as_delta_table(
            staging,
            context.op_config["dataset_type"],
            staging_table_path,
            spark.spark_session,
        )

    emit_metadata_to_datahub(context, staging)
