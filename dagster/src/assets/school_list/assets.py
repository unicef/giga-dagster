import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import SparkSession
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.internal.common_assets.staging import staging_step
from src.resources import ResourceKey
from src.spark.transform_functions import (
    column_mapping_rename,
    create_bronze_layer_columns,
)
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.apis import query_api_data
from src.utils.datahub.create_validation_tab import (
    datahub_emit_assertions_with_exception_catcher,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db import get_db_context
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import (
    get_schema_columns,
    get_schema_columns_datahub,
)
from src.utils.send_email_dq_report import send_email_dq_report_with_config

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_list_raw(
    context: OpExecutionContext, config: FileConfig, spark: PySparkResource
) -> Output[pd.DataFrame]:
    context.log.info(f"Database row: {config.row_data_dict}")
    with get_db_context() as database_session:
        df = pd.DataFrame.from_records(
            query_api_data(context, database_session, config.row_data_dict),
        )

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=df,
    )
    return Output(df, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_list_bronze(
    context: OpExecutionContext,
    qos_school_list_raw: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session
    schema_columns = get_schema_columns(s, config.metastore_schema)
    df, column_mapping = column_mapping_rename(
        qos_school_list_raw, config.row_data_dict["column_to_schema_mapping"]
    )

    country_code = config.country_code
    df = create_bronze_layer_columns(df, schema_columns, country_code)

    config.metadata.update({"column_mapping": column_mapping})

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=df,
    )

    df_pandas = df.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "column_mapping": column_mapping,
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_list_data_quality_results(
    context: OpExecutionContext,
    config: FileConfig,
    qos_school_list_bronze: sql.DataFrame,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )

    country_code = config.country_code
    dq_results = row_level_checks(
        qos_school_list_bronze,
        "geolocation",
        country_code,
        context,
    )

    dq_pandas = dq_results.toPandas()
    return Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(dq_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
def qos_school_list_data_quality_results_summary(
    context: OpExecutionContext,
    qos_school_list_bronze: sql.DataFrame,
    qos_school_list_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[dict]:
    dq_summary_statistics = aggregate_report_json(
        aggregate_report_spark_df(
            spark.spark_session,
            qos_school_list_data_quality_results,
        ),
        qos_school_list_bronze,
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
def qos_school_list_dq_passed_rows(
    context: OpExecutionContext,
    qos_school_list_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    df_passed = dq_split_passed_rows(
        qos_school_list_data_quality_results,
        "geolocation",
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
def qos_school_list_dq_failed_rows(
    context: OpExecutionContext,
    qos_school_list_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    df_failed = dq_split_failed_rows(
        qos_school_list_data_quality_results,
        "geolocation",
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
def qos_school_list_staging(
    context: OpExecutionContext,
    qos_school_list_dq_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[None]:
    staging = staging_step(
        context,
        config,
        adls_file_client,
        spark.spark_session,
        upstream_df=qos_school_list_dq_passed_rows,
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

    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(staging),
        },
    )
