import time

import pandas as pd
from dagster_ge.factory import GEContextResource
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark import sql
from src.spark import transform_functions as tf
from src.utils.adls import ADLSFileClient, get_filepath, get_output_filepath

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_raw_io_manager")
def raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
) -> pd.DataFrame:
    df = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"]
    )
    # # emit_metadata_to_datahub(context, df=df)
    yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})


@asset(io_manager_key="adls_delta_io_manager")
def bronze(context: OpExecutionContext, raw: sql.DataFrame) -> sql.DataFrame:
    df = tf.create_bronze_layer_columns(raw)
    # emit_metadata_to_datahub(context, df=raw)
    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(
    io_manager_key="adls_delta_io_manager",
    op_tags={"kind": "ge"},
)
def data_quality_results(
    context,
    bronze: sql.DataFrame,
    gx: GEContextResource,
):
    validations = [
        {
            "batch_request": {
                "datasource_name": "spark_datasource",
                "runtime_parameters": {"batch_data": bronze},
                "data_connector_name": "runtime_data_connector",
                "data_asset_name": "bronze_school_data",
                "batch_identifiers": {
                    "name": get_output_filepath(context),
                    "step": "bronze",
                },
            },
            "expectation_suite_name": "expectation_school_geolocation",
        },
    ]
    dq_results = gx.get_data_context().run_checkpoint(
        checkpoint_name="school_geolocation_checkpoint", validations=validations
    )
    yield Output(
        dq_results.to_json_dict(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_delta_io_manager")
def dq_passed_rows(
    context: OpExecutionContext,
    bronze: sql.DataFrame,
    data_quality_results,
) -> sql.DataFrame:
    df_passed = tf.dq_passed_rows(bronze, data_quality_results)
    df_passed = df_passed.drop("gx_index")
    # emit_metadata_to_datahub(context, df_passed)
    yield Output(df_passed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def dq_failed_rows(
    context: OpExecutionContext,
    bronze: sql.DataFrame,
    data_quality_results,
) -> sql.DataFrame:
    df_failed = tf.dq_failed_rows(bronze, data_quality_results)
    df_failed = df_failed.drop("gx_index")
    # emit_metadata_to_datahub(context, df_failed)
    yield Output(df_failed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def staging(
    context: OpExecutionContext,
    dq_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    silver_table_path = get_filepath(
        context.run_tags["dagster/run_key"], dataset_type, "silver"
    )

    filepaths_with_modified_date = []
    for file_data in adls_file_client.list_paths(
        f"staging/pending-review/{dataset_type}"
    ):
        if file_data["is_directory"]:
            continue
        else:
            properties = adls_file_client.get_file_metadata(
                context.run_tags["dagster/run_key"]
            )
            filepath = file_data["name"]
            if (
                filepath.split("/")[-1].split("_")[0]
                == context.run_tags["dagster/run_key"].split("/")[-1].split("_")[0]
            ):
                date_modified = properties["metadata"]["Date_Modified"]
                filepaths_with_modified_date.append(
                    {"filepath": filepath, "date_modified": date_modified}
                )

    filepaths_with_modified_date.sort(
        key=lambda x: time.mktime(
            time.strptime(x["date_modified"], "%d/%m/%Y %H:%M:%S")
        )
    )

    # fix this, .execute() is inplace in ADLS
    if DeltaTable.isDeltaTable(spark, silver_table_path):
        # Clone silver table to staging folder
        silver = adls_file_client.download_delta_table_as_spark_dataframe(
            silver_table_path, spark.spark_session
        )

        staging_table_path = get_filepath(
            context.run_tags["dagster/run_key"], dataset_type, "staging"
        )
        adls_file_client.upload_spark_dataframe_as_delta_table_within_dagster(
            context, silver, staging_table_path, spark.spark_session
        )

        # Load silver table in staging folder as a deltatable
        staging = adls_file_client.download_delta_table_as_delta_table(
            staging_table_path, spark.spark_session
        )

        # Merge each pending file for the same country
        for file_date in filepaths_with_modified_date:
            existing_file = adls_file_client.download_delta_table_as_spark_dataframe(
                file_date["filepath"], spark.spark_session
            )

            staging.alias("source").merge(
                existing_file.alias("target"),
                "source.giga_id_school = target.giga_id_school",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        staging = staging.toDF()

    else:
        staging = dq_passed_rows
        # If no existing silver table, just merge the spark dataframes
        for file_date in filepaths_with_modified_date:
            existing_file = adls_file_client.download_delta_table_as_spark_dataframe(
                file_date["filepath"], spark.spark_session
            )
            staging = staging.union(existing_file)

    # emit_metadata_to_datahub(context, staging)
    yield Output(staging, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def manual_review_passed_rows(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    approved_table_path = get_output_filepath(context)
    df = adls_file_client.download_delta_table_as_spark_dataframe(
        approved_table_path, spark.spark_session
    )
    # emit_metadata_to_datahub(context, df)
    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def manual_review_failed_rows(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    rejected_table_path = get_output_filepath(context)
    df = adls_file_client.download_delta_table_as_spark_dataframe(
        rejected_table_path, spark.spark_session
    )
    # emit_metadata_to_datahub(context, df)
    yield Output(df, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def silver(
    context: OpExecutionContext,
    manual_review_passed_rows: sql.DataFrame,
) -> sql.DataFrame:
    silver = manual_review_passed_rows
    # emit_metadata_to_datahub(context, silver)
    yield Output(silver, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def gold(context: OpExecutionContext, silver: sql.DataFrame) -> sql.DataFrame:
    gold = silver
    # emit_metadata_to_datahub(context, gold)
    yield Output(gold, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def master_csv_to_gold(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    df_pandas = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"]
    )

    df_spark = adls_file_client.download_csv_as_spark_dataframe(
        context.run_tags["dagster/run_key"], spark.spark_session
    )

    if len(df_pandas.columns) == len(df_spark.columns):
        context.log.info("Column count matches")
    else:
        raise Exception(
            f"Column count mismatch: pandas={len(df_pandas.columns)}, spark={len(df_spark.columns)}"
        )
    if len(df_pandas) == df_spark.count():
        context.log.info("Row count matches")
    else:
        raise Exception(
            f"Row count mismatch: pandas={len(df_pandas)}, spark={df_spark.count()}"
        )
    if (
        df_pandas.isnull().sum().to_dict()
        == df_spark.toPandas().isnull().sum().to_dict()
    ):
        context.log.info("Null count matches")
    else:
        raise Exception(
            f"Null count mismatch: pandas={df_pandas.isnull().sum().to_dict()}, spark={df_spark.toPandas().isnull().sum().to_dict()}"
        )

    yield Output(df_spark, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_io_manager")
def reference_csv_to_gold(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> sql.DataFrame:
    df = adls_file_client.download_csv_as_spark_dataframe(
        context.run_tags["dagster/run_key"], spark.spark_session
    )
    yield Output(df, metadata={"filepath": get_output_filepath(context)})
