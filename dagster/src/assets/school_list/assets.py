import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from src.constants import DataTier
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.internal.common_assets.staging import StagingChangeTypeEnum, StagingStep
from src.resources import ResourceKey
from src.spark.transform_functions import (
    column_mapping_rename,
    create_bronze_layer_columns,
)
from src.utils.adls import (
    ADLSFileClient,
)

# from src.utils.datahub.create_validation_tab import (
#     datahub_emit_assertions_with_exception_catcher,
# )
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db.primary import get_db_context
from src.utils.delta import check_table_exists
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.qos_apis.school_list import query_school_list_data
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_schema_columns,
    get_schema_columns_datahub,
)

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_list_raw(
    context: OpExecutionContext, config: FileConfig
) -> Output[pd.DataFrame]:
    context.log.info(f"Database row: {config.row_data_dict}")
    with get_db_context() as database_session:
        df = pd.DataFrame.from_records(
            query_school_list_data(context, database_session, config.row_data_dict),
        )

    # datahub_emit_metadata_with_exception_catcher(
    #     context=context,
    #     config=config,
    # )

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df),
            "preview": get_table_preview(df),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def qos_school_list_bronze(
    context: OpExecutionContext,
    qos_school_list_raw: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session
    country_code = config.country_code
    schema_name = config.metastore_schema

    database_data = config.row_data_dict
    df, column_mapping = column_mapping_rename(
        qos_school_list_raw, database_data["column_to_schema_mapping"]
    )

    columns = get_schema_columns(s, schema_name)
    schema = StructType(columns)

    if check_table_exists(s, schema_name, country_code, DataTier.SILVER):
        silver_tier_schema_name = construct_schema_name_for_tier(
            "school_geolocation", DataTier.SILVER
        )
        silver_table_name = construct_full_table_name(
            silver_tier_schema_name, country_code
        )
        silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    else:
        silver = s.createDataFrame(s.sparkContext.emptyRDD(), schema=schema)

    df = create_bronze_layer_columns(df, silver, country_code)

    config.metadata.update({"column_mapping": column_mapping})

    # datahub_emit_metadata_with_exception_catcher(
    #     context=context,
    #     config=config,
    #     spark=spark,
    #     schema_reference=df,
    # )

    df_pandas = df.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df_pandas),
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
    s: SparkSession = spark.spark_session
    country_code = config.country_code
    schema_name = config.metastore_schema

    columns = get_schema_columns(s, schema_name)
    schema = StructType(columns)

    if check_table_exists(s, schema_name, country_code, DataTier.SILVER):
        silver_tier_schema_name = construct_schema_name_for_tier(
            "school_geolocation", DataTier.SILVER
        )
        silver_table_name = construct_full_table_name(
            silver_tier_schema_name, country_code
        )
        silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    else:
        silver = s.createDataFrame(s.sparkContext.emptyRDD(), schema=schema)

    dq_results = row_level_checks(
        df=qos_school_list_bronze,
        dataset_type="geolocation",
        _country_code_iso3=config.country_code,
        silver=silver,
        context=context,
    )

    dq_pandas = dq_results.toPandas()

    return Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(dq_pandas),
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

    # datahub_emit_assertions_with_exception_catcher(
    #     context=context, dq_summary_statistics=dq_summary_statistics
    # )
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

    # schema_reference = get_schema_columns_datahub(
    #     spark.spark_session,
    #     config.metastore_schema,
    # )
    # datahub_emit_metadata_with_exception_catcher(
    #     context=context,
    #     config=config,
    #     spark=spark,
    #     schema_reference=schema_reference,
    # )

    df_pandas = df_passed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df_pandas),
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

    df_pandas = df_failed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df_pandas),
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
    if qos_school_list_dq_passed_rows.count() == 0:
        context.log.warning("Skipping staging as there are no rows passing DQ checks")
        return Output(None)

    staging_step = StagingStep(
        context,
        config,
        adls_file_client,
        spark.spark_session,
        StagingChangeTypeEnum.UPDATE,
    )
    staging = staging_step(upstream_df=qos_school_list_dq_passed_rows)
    row_count = 0 if staging is None else staging.count()

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
            "row_count": row_count,
            "preview": get_table_preview(staging),
        },
    )
