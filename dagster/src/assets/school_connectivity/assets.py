# import pandas as pd
# from dagster_pyspark import PySparkResource
# from delta import DeltaTable
# from pyspark import sql
# from pyspark.sql import SparkSession
# from src.constants import DataTier
# from src.data_quality_checks.utils import (
#     aggregate_report_json,
#     aggregate_report_spark_df,
#     dq_split_failed_rows,
#     dq_split_passed_rows,
#     row_level_checks,
# )
# from src.resources import ResourceKey
# from src.utils.adls import (
#     ADLSFileClient,
# )
# from src.utils.datahub.create_validation_tab import (
#     datahub_emit_assertions_with_exception_catcher,
# )
# from src.utils.datahub.emit_dataset_metadata import (
#     datahub_emit_metadata_with_exception_catcher,
# )
# from src.utils.db import get_db_context
# from src.utils.metadata import get_output_metadata, get_table_preview
# from src.utils.op_config import FileConfig
# from src.utils.qos_apis.school_connectivity import query_school_connectivity_API_data
# from src.utils.schema import (
#     construct_full_table_name,
#     construct_schema_name_for_tier,
#     get_schema_columns_datahub,
# )
# from src.utils.send_email_dq_report import send_email_dq_report_with_config

# from dagster import OpExecutionContext, Output, asset


# @asset(io_manager_key="adls_pandas_io_manager")
# def qos_school_connectivity_raw(
#     context: OpExecutionContext,
#     config: FileConfig,
#     spark: PySparkResource,
# ) -> Output[pd.DataFrame]:
#     s: SparkSession = spark.spark_session
#     context.log.info(f"Database row: {config.row_data_dict}")
#     database_data = config.row_data_dict

#     country_code = "RWA"

#     if database_data.school_id_key and database_data.school_list.school_id_key:
#         school_ids = set()

#         schema_name = "school_geolocation"
#         silver_tier_schema_name = construct_schema_name_for_tier(
#             schema_name, DataTier.SILVER
#         )
#         silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)
#         silver = DeltaTable.forName(s, silver_table_name).toDF()
#         current_school_ids = (
#             silver.select(silver(database_data.school_list.school_id_key))
#             .distinct()
#             .collect()
#         )

# ## Shouldn't be here, it's new data + archived data
#         archived_files_schema_name = construct_schema_name_for_tier('qos', DataTier.RAW)
#         archived_table_name = construct_full_table_name(archived_files_schema_name, country_code)

#         archived_file = DeltaTable.forName(s, archived_table_name).toDF()

#         archived_school_ids = (
#             archived_file.select(archived_file(database_data.school_list.school_id_key))
#             .distinct()
#             .collect()
#         )

#         school_ids.update(current_school_ids)
#         school_ids.update(archived_school_ids)

#         df = pd.DataFrame()
#         with get_db_context() as database_session:
#             for id in school_ids:
#                 data = pd.DataFrame.from_records(
#                     query_school_connectivity_API_data(
#                         context, database_session, config.row_data_dict, id
#                     ),
#                 )
#                 df = df.append(data, ignore_index=True)

#     else:
#         df = pd.DataFrame.from_records(
#             query_school_connectivity_API_data(
#                 context, database_session, config.row_data_dict
#             ),
#         )

#     datahub_emit_metadata_with_exception_catcher(
#         context=context,
#         config=config,
#         spark=spark,
#         schema_reference=df,
#     )
#     return Output(df, **get_output_metadata(config))


# @asset(io_manager_key="adls_pandas_io_manager")
# def qos_school_connectivity_bronze(
#     context: OpExecutionContext,
#     qos_school_connectivity_raw: sql.DataFrame,
#     spark: PySparkResource,
#     config: FileConfig,
# ) -> Output[pd.DataFrame]:
#     ## @RENZ NEED TO ADD GIGA_SCHOOL_ID
#     # df = create_bronze_layer_columns(qos_school_connectivity_raw)

#     datahub_emit_metadata_with_exception_catcher(
#         context=context,
#         config=config,
#         spark=spark,
#         schema_reference=df,
#     )
#     return Output(df.toPandas(), metadata=get_output_metadata(config))


# @asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
# def qos_school_connectivity_data_quality_results(
#     context: OpExecutionContext,
#     config: FileConfig,
#     qos_school_connectivity_bronze: sql.DataFrame,
#     spark: PySparkResource,
# ) -> Output[pd.DataFrame]:
#     datahub_emit_metadata_with_exception_catcher(
#         context=context,
#         config=config,
#         spark=spark,
#     )

#     country_code = config.country_code
#     dq_results = row_level_checks(
#         qos_school_connectivity_bronze,
#         "qos",
#         country_code,
#         context,
#     )

#     dq_pandas = dq_results.toPandas()
#     return Output(
#         dq_pandas,
#         metadata={
#             **get_output_metadata(config),
#             "preview": get_table_preview(dq_pandas),
#         },
#     )


# @asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
# def qos_school_connectivity_data_quality_results_summary(
#     context: OpExecutionContext,
#     qos_school_connectivity_bronze: sql.DataFrame,
#     qos_school_connectivity_data_quality_results: sql.DataFrame,
#     spark: PySparkResource,
#     config: FileConfig,
# ) -> Output[dict]:
#     dq_summary_statistics = aggregate_report_json(
#         aggregate_report_spark_df(
#             spark.spark_session,
#             qos_school_connectivity_data_quality_results,
#         ),
#         qos_school_connectivity_bronze,
#     )

#     datahub_emit_assertions_with_exception_catcher(
#         context=context, dq_summary_statistics=dq_summary_statistics
#     )
#     datahub_emit_metadata_with_exception_catcher(
#         context=context,
#         config=config,
#         spark=spark,
#     )

#     send_email_dq_report_with_config(
#         dq_results=dq_summary_statistics,
#         config=config,
#         context=context,
#     )

#     return Output(dq_summary_statistics, metadata=get_output_metadata(config))


# @asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
# def qos_school_connectivity_dq_passed_rows(
#     context: OpExecutionContext,
#     qos_school_connectivity_data_quality_results: sql.DataFrame,
#     config: FileConfig,
#     spark: PySparkResource,
# ) -> Output[pd.DataFrame]:
#     df_passed = dq_split_passed_rows(
#         qos_school_connectivity_data_quality_results,
#         "geolocation",
#     )

#     schema_reference = get_schema_columns_datahub(
#         spark.spark_session,
#         config.metastore_schema,
#     )
#     datahub_emit_metadata_with_exception_catcher(
#         context=context,
#         config=config,
#         spark=spark,
#         schema_reference=schema_reference,
#     )

#     df_pandas = df_passed.toPandas()
#     return Output(
#         df_pandas,
#         metadata={
#             **get_output_metadata(config),
#             "preview": get_table_preview(df_pandas),
#         },
#     )


# @asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
# def qos_school_connectivity_dq_failed_rows(
#     context: OpExecutionContext,
#     qos_school_connectivity_data_quality_results: sql.DataFrame,
#     config: FileConfig,
#     spark: PySparkResource,
# ) -> Output[pd.DataFrame]:
#     df_failed = dq_split_failed_rows(
#         qos_school_connectivity_data_quality_results,
#         "geolocation",
#     )

#     schema_reference = get_schema_columns_datahub(
#         spark.spark_session,
#         config.metastore_schema,
#     )
#     datahub_emit_metadata_with_exception_catcher(
#         context=context,
#         config=config,
#         spark=spark,
#         schema_reference=schema_reference,
#         df_failed=df_failed,
#     )

#     df_pandas = df_failed.toPandas()
#     return Output(
#         df_pandas,
#         metadata={
#             **get_output_metadata(config),
#             "preview": get_table_preview(df_pandas),
#         },
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
#     yield Output(silver, metadata=get_output_metadata(config))


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
#     yield Output(gold, metadata=get_output_metadata(config))


# # Get date key, date send in, date format
# # Send date request in send_in, following date_format
# # How will you know the increment value for the date? e.g. hourly, daily, etc (DAY LEVEL PARTITION)
# # How will you keep track of backfill status without a separate partitioned ingestion pipeline? (JUST TODAY'S DATE)
