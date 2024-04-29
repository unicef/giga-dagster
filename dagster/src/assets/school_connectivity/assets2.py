# import pandas as pd
# from dagster_pyspark import PySparkResource
# from delta import DeltaTable
# from pyspark import sql
# from schemas.qos import SchoolConnectivityConfig
# from src.data_quality_checks.utils import (
#     aggregate_report_json,
#     aggregate_report_spark_df,
#     row_level_checks,
# )
# from src.sensors.base import FileConfig
# from src.settings import settings
# from src.spark.transform_functions import create_bronze_layer_columns
# from src.utils.adls import ADLSFileClient, get_filepath, get_output_filepath
# from dagster.src.utils.apis.school_connectivity import query_school_connectivity_API_data
# from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub

# from dagster import AssetOut, OpExecutionContext, Output, asset, multi_asset


# @asset(io_manager_key="adls_pandas_io_manager")
# def qos_school_connectivity_raw(
#     context: OpExecutionContext,
#     adls_file_client: ADLSFileClient,
#     spark: PySparkResource,
#     config: SchoolConnectivityConfig,
# ) -> pd.DataFrame:
#     # Is QOS connectivity ingestion by country? Not sure which country to select if it's not
#     school_list_filename = f"{settings.AZURE_BLOB_CONNECTION_URI}/silver/school-list-data/{config['school_list']['name']}"

#     school_list_data = adls_file_client.download_delta_table_as_spark_dataframe(
#         school_list_filename, spark.spark_session
#     ).toPandas()

#     archived_files = adls_file_client.list_paths(
#         "archive/missing-giga-school-id"
#     )  # should we check all files? I think we'll get a lot of duplicates from previous runs -- should we just get the most recent archived file?

#     archived_files.sort(key=lambda x: x["date_modified"], reverse=True)

#     # Get the most recent archived file (should have all archived rows)
#     school_list_data_archived = (
#         adls_file_client.download_delta_table_as_spark_dataframe(
#             f"{settings.AZURE_BLOB_CONNECTION_URI}/{archived_files[0]}",
#             spark.spark_session,
#         ).toPandas()
#     )

#     school_ids_new: list[int] = school_list_data[
#         config["school_list"]["school_id_key"]
#     ].to_list()
#     school_ids_archived: list[int] = school_list_data_archived[
#         config["school_list"]["school_id_key"]
#     ].to_list()

#     school_ids = school_ids_new.extend(school_ids_archived)

#     df = pd.DataFrame()
#     for id in school_ids:
#         school_connectivity_data = pd.DataFrame.from_records(
#             query_school_connectivity_API_data(context, config, id)
#         )

#         df = df.append(school_connectivity_data, ignore_index=True)

#     emit_metadata_to_datahub(context, df=df)
#     yield Output(df, metadata={"filepath": context.run_tags["dagster/run_key"]})


# @asset(io_manager_key="adls_pandas_io_manager")
# def qos_school_connectivity_bronze(
#     context: OpExecutionContext,
#     qos_school_connectivity_raw: sql.DataFrame,
# ) -> pd.DataFrame:
#     ## @RENZ NEED TO ADD GIGA_SCHOOL_ID
#     df = create_bronze_layer_columns(qos_school_connectivity_raw)
#     emit_metadata_to_datahub(context, df=qos_school_connectivity_raw)
#     yield Output(df.toPandas(), metadata={"filepath": get_output_filepath(context)})


# @multi_asset(
#     outs={
#         "qos_school_connectivity_dq_results": AssetOut(
#             is_required=True, io_manager_key="adls_pandas_io_manager"
#         ),
#         "qos_school_connectivity_dq_summary_statistics": AssetOut(
#             is_required=True, io_manager_key="adls_json_io_manager"
#         ),
#     }
# )  # @RENZ WILL WE STILL HAVE STANDARD DQ CHECKS?
# def qos_school_connectivity_data_quality_results(
#     context,
#     config: FileConfig,
#     qos_school_connectivity_bronze: sql.DataFrame,
#     spark: PySparkResource,
# ):
#     country_code = context.run_tags["dagster/run_key"].split("/")[-1].split("_")[1]
#     dq_results = row_level_checks(
#         qos_school_connectivity_bronze, "geolocation", country_code
#     )
#     dq_summary_statistics = aggregate_report_json(
#         aggregate_report_spark_df(spark.spark_session, dq_results),
#         qos_school_connectivity_bronze,
#     )

#     yield Output(
#         dq_results.toPandas(),
#         metadata={
#             "filepath": get_output_filepath(
#                 context, "qos_school_connectivity_dq_results"
#             )
#         },
#         output_name="qos_school_connectivity_dq_results",
#     )

#     yield Output(
#         dq_summary_statistics,
#         metadata={
#             "filepath": get_output_filepath(
#                 context, "qos_school_connectivity_dq_summary_statistics"
#             )
#         },
#         output_name="qos_school_connectivity_dq_summary_statistics",
#     )


# @asset(io_manager_key="adls_pandas_io_manager")
# def qos_school_connectivity_dq_passed_rows(
#     context: OpExecutionContext,
#     qos_school_connectivity_dq_results: sql.DataFrame,
# ) -> sql.DataFrame:
#     df_passed = qos_school_connectivity_dq_results
#     emit_metadata_to_datahub(context, df_passed)
#     yield Output(
#         df_passed.toPandas(), metadata={"filepath": get_output_filepath(context)}
#     )


# @asset(io_manager_key="adls_pandas_io_manager")
# def qos_school_connectivity_dq_failed_rows(
#     context: OpExecutionContext,
#     qos_school_connectivity_dq_results: sql.DataFrame,
# ) -> sql.DataFrame:
#     df_failed = qos_school_connectivity_dq_results
#     emit_metadata_to_datahub(context, df_failed)
#     yield Output(
#         df_failed.toPandas(), metadata={"filepath": get_output_filepath(context)}
#     )


# @asset(io_manager_key="adls_delta_io_manager")
# def qos_school_connectivity_silver(
#     context: OpExecutionContext,
#     qos_school_connectivity_dq_passed_rows: sql.DataFrame,
#     adls_file_client: ADLSFileClient,
#     spark: PySparkResource,
# ) -> sql.DataFrame:
#     dataset_type = context.get_step_execution_context().op_config["dataset_type"]
#     filepath = context.run_tags["dagster/run_key"].split("/")[-1]
#     silver_table_name = filepath.split("/")[-1].split("_")[1]
#     silver_table_path = (
#         f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'silver').split('/')[:-1]}/{silver_table_name}",
#     )

#     if DeltaTable.isDeltaTable(spark.spark_session, silver_table_path):
#         silver = adls_file_client.download_delta_table_as_delta_table(
#             silver_table_path, spark.spark_session
#         )

#         silver = (
#             silver.alias("source")
#             .merge(
#                 qos_school_connectivity_dq_passed_rows.alias("target"),
#                 "source.school_id_giga = target.school_id_giga",
#             )
#             .whenMatchedUpdateAll()
#             .whenNotMatchedInsertAll()
#             .execute()
#         )

#     emit_metadata_to_datahub(context, df=qos_school_connectivity_dq_passed_rows)
#     yield Output(silver, metadata={"filepath": get_output_filepath(context)})


# @asset
# def qos_school_connectivity_gold(
#     context: OpExecutionContext,
#     qos_school_connectivity_silver: sql.DataFrame,
#     adls_file_client: ADLSFileClient,
#     spark: PySparkResource,
# ) -> sql.DataFrame:
#     dataset_type = context.get_step_execution_context().op_config["dataset_type"]
#     filepath = context.run_tags["dagster/run_key"].split("/")[-1]
#     gold_table_name = filepath.split("/")[-1].split("_")[1]
#     gold_table_path = (
#         f"{settings.AZURE_BLOB_CONNECTION_URI}/{get_filepath(filepath, dataset_type, 'gold').split('/')[:-1]}/{gold_table_name}",
#     )

#     if DeltaTable.isDeltaTable(spark.spark_session, gold_table_path):
#         gold = adls_file_client.download_delta_table_as_delta_table(
#             gold_table_path, spark.spark_session
#         )
# Comment on lines +200 to +202
# @tm-kenneth-domingo tm-kenneth-domingo on Mar 12
# Delta Tables should be referenced by name so that they can be accessed by downstream applications (Trino, Superset).

# example

# When referencing by name, they are saved in the following location:

# {settings.SPARK_WAREHOUSE_DIR}/{schema_name}.db/{table_name}
# In the deployed environment, SPARK_WAREHOUSE_DIR is at giga-dataops-{env}/warehouse. For local, it's at giga-dataops-dev/warehouse-local.

# The schema_name is determined in your op config under metastore_schema and directly corresponds to a schema delta table in ADLS ({settings.SPARK_WAREHOUSE_DIR}/schemas.db)

# Author
# @tm-erin-cheng tm-erin-cheng on Mar 15
# is this for all? In other files that's how we've been downloading the delta tables too

# @tm-erin-cheng	Reply...

#         gold = (
#             gold.alias("source")
#             .merge(
#                 qos_school_connectivity_silver.alias("target"),
#                 "source.school_id_giga = target.school_id_giga",
#             )
#             .whenMatchedUpdateAll()
#             .whenNotMatchedInsertAll()
#             .execute()
#         )

#     emit_metadata_to_datahub(context, df=qos_school_connectivity_dq_passed_rows)
#     yield Output(gold, metadata={"filepath": get_output_filepath(context)})


# # Get date key, date send in, date format
# # Send date request in send_in, following date_format
# # How will you know the increment value for the date? e.g. hourly, daily, etc (DAY LEVEL PARTITION)
# # How will you keep track of backfill status without a separate partitioned ingestion pipeline? (JUST TODAY'S DATE)
