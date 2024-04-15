from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark import sql
from pyspark.sql import SparkSession
from src.constants import DataTier
from src.resources import ResourceKey
from src.spark.transform_functions import add_missing_columns
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_primary_key,
    get_schema_columns,
    get_schema_columns_datahub,
)
from src.utils.spark import compute_row_hash, transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def manual_review_failed_rows(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    s: SparkSession = spark.spark_session
    passing_row_ids = adls_file_client.download_json(config.filepath)

    schema_name = config.metastore_schema
    country_code = config.country_code
    primary_key = get_primary_key(s, schema_name)
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )

    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )

    staging = DeltaTable.forName(s, staging_table_name).alias("staging").toDF()

    df_failed = staging.filter(~staging[primary_key].isin(passing_row_ids))

    schema_reference = get_schema_columns_datahub(s, schema_name)

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )
    yield Output(df_failed, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def silver(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
):
    s: SparkSession = spark.spark_session
    passing_row_ids = adls_file_client.download_json(config.filepath)

    schema_name = config.metastore_schema
    country_code = config.country_code
    primary_key = get_primary_key(s, schema_name)
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )

    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )

    staging = DeltaTable.forName(s, staging_table_name).alias("staging").toDF()
    df_passed = staging.filter(staging[primary_key].isin(passing_row_ids))

    schema_reference = get_schema_columns_datahub(s, schema_name)

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    yield Output(
        df_passed,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_passed),
        },
    )


@asset(
    io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value,
)
def master(
    context: OpExecutionContext,
    silver: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
):
    s: SparkSession = spark.spark_session
    schema_name = config.metastore_schema
    schema_columns = get_schema_columns(s, schema_name)

    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, schema_name, context)
    silver = compute_row_hash(silver)
    silver = silver.select([c.name for c in schema_columns])

    schema_reference = get_schema_columns_datahub(s, schema_name)
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=s,
        schema_reference=schema_reference,
    )
    yield Output(
        silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(silver),
        },
    )  ## @QUESTION: Not sure what to put here if it's inplace write in delta


@asset(
    io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value,
)
def reference(
    context: OpExecutionContext,
    silver: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
):
    s: SparkSession = spark.spark_session
    schema_name = config.metastore_schema
    schema_columns = get_schema_columns(s, schema_name)

    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, schema_name, context)
    silver = compute_row_hash(silver)
    silver = silver.select([c.name for c in schema_columns])

    schema_reference = get_schema_columns_datahub(s, schema_name)
    datahub_emit_metadata_with_exception_catcher(
        context,
        schema_reference=schema_reference,
        country_code=config.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )

    yield Output(
        silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(silver),
        },
    )  ## @QUESTION: Not sure what to put here if it's inplace write in delta
