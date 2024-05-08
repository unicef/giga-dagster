import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark import sql
from pyspark.sql import SparkSession
from src.constants import DataTier
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.resources import ResourceKey
from src.utils.datahub.create_validation_tab import (
    datahub_emit_assertions_with_exception_catcher,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db import get_db_context
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.qos_apis.school_connectivity import query_school_connectivity_data
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_schema_columns_datahub,
)
from src.utils.send_email_dq_report import send_email_dq_report_with_config

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_pandas_io_manager")
def qos_school_connectivity_raw(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session
    context.log.info(f"Database row: {config.row_data_dict}")
    database_data = config.row_data_dict

    complete_school_id_parameters = bool(
        database_data["school_id_key"]
        and database_data["school_list"]["school_id_key"]
        and database_data["school_id_send_query_in"] != "NONE"
    )

    df = pd.DataFrame()

    if complete_school_id_parameters:
        schema_name = "school_geolocation"
        silver_tier_schema_name = construct_schema_name_for_tier(
            schema_name, DataTier.SILVER
        )
        silver_table_name = construct_full_table_name(
            silver_tier_schema_name, config.country_code
        )

        if spark.catalog.tableExists(silver_table_name):
            silver = DeltaTable.forName(s, silver_table_name).toDF()
            unique_school_ids = (
                silver.select(silver(database_data["school_list"]["school_id_key"]))
                .distinct()
                .collect()
            )

            with get_db_context() as database_session:
                for id in unique_school_ids:
                    data = pd.DataFrame.from_records(
                        query_school_connectivity_data(
                            context, database_session, database_data, id
                        ),
                    )
                    df = df.append(data, ignore_index=True)

    else:
        with get_db_context() as database_session:
            df = pd.DataFrame.from_records(
                query_school_connectivity_data(
                    context, database_session, database_data
                ),
            )

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=df,
    )

    context.log.info(f">> df: {df}")
    return Output(df, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_connectivity_bronze(
    context: OpExecutionContext,
    qos_school_connectivity_raw: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    # s: SparkSession = spark.spark_session
    # database_data = config.row_data_dict

    # schema_name = "school_geolocation"
    # silver_tier_schema_name = construct_schema_name_for_tier(
    #     schema_name, DataTier.SILVER
    # )
    # silver_table_name = construct_full_table_name(
    #     silver_tier_schema_name, config.country_code
    # )

    # bronze = pd.DataFrame()
    bronze = qos_school_connectivity_raw.toPandas()

    # if s.catalog.tableExists(silver_table_name):
    #     context.log.info(f"table exists")
    #     silver = DeltaTable.forName(s, silver_table_name).toDF()
    #     primary_key = get_primary_key(s, schema_name)
    #     context.log.info(f"pkey: {primary_key}")

    #     unique_school_ids = (
    #         silver.select(silver(database_data.school_list.school_id_key))
    #         .distinct()
    #         .collect()
    #     )

    #     context.log.info(f"unique: {unique_school_ids}")

    #     bronze = qos_school_connectivity_raw.filter(qos_school_connectivity_raw[primary_key].isin(unique_school_ids)).toPandas()
    #     context.log.info(f"qos raw: {len(qos_school_connectivity_raw.toPandas())}, qos bronze: {len(qos_school_connectivity_bronze)}")

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )

    return Output(
        bronze,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(bronze),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_connectivity_data_quality_results(
    context: OpExecutionContext,
    config: FileConfig,
    qos_school_connectivity_bronze: sql.DataFrame,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    country_code = config.country_code
    dq_results = row_level_checks(
        qos_school_connectivity_bronze,
        "qos",
        country_code,
        context,
    )

    dq_pandas = dq_results.toPandas()
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )
    return Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(dq_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
def qos_school_connectivity_data_quality_results_summary(
    context: OpExecutionContext,
    qos_school_connectivity_raw: sql.DataFrame,
    qos_school_connectivity_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[dict]:
    dq_summary_statistics = aggregate_report_json(
        aggregate_report_spark_df(
            spark.spark_session,
            qos_school_connectivity_data_quality_results,
        ),
        qos_school_connectivity_raw,
    )

    datahub_emit_assertions_with_exception_catcher(
        context=context, dq_summary_statistics=dq_summary_statistics
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )

    send_email_dq_report_with_config(
        dq_results=dq_summary_statistics,
        config=config,
        context=context,
    )

    return Output(dq_summary_statistics, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_connectivity_dq_passed_rows(
    context: OpExecutionContext,
    qos_school_connectivity_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    df_passed = dq_split_passed_rows(
        qos_school_connectivity_data_quality_results,
        "qos",
    )

    schema_reference = get_schema_columns_datahub(
        spark.spark_session,
        config.metastore_schema,
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    df_pandas = df_passed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_connectivity_dq_failed_rows(
    context: OpExecutionContext,
    qos_school_connectivity_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    df_failed = dq_split_failed_rows(
        qos_school_connectivity_data_quality_results,
        "qos",
    )

    schema_reference = get_schema_columns_datahub(
        spark.spark_session,
        config.metastore_schema,
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
        df_failed=df_failed,
    )

    df_pandas = df_failed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def qos_school_connectivity_gold(
    context: OpExecutionContext,
    qos_school_connectivity_dq_passed_rows: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
    return Output(
        qos_school_connectivity_dq_passed_rows,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(qos_school_connectivity_dq_passed_rows),
        },
    )


# # Get date key, date send in, date format
# # Send date request in send_in, following date_format
# # How will you know the increment value for the date? e.g. hourly, daily, etc (DAY LEVEL PARTITION)
# # How will you keep track of backfill status without a separate partitioned ingestion pipeline? (JUST TODAY'S DATE)
