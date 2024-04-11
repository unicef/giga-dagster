from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark import sql
from src.constants import DataTier
from src.resources import ResourceKey
from src.spark.transform_functions import add_missing_columns
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.delta import (
    build_deduped_merge_query,
    execute_query_with_error_handler,
)
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_primary_key,
    get_schema_columns,
    get_schema_columns_datahub,
    get_schema_columns_master,
    get_schema_columns_reference,
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
    passing_row_ids = adls_file_client.download_json(config.filepath)

    schema_name = config.metastore_schema
    country_code = config.filename_components.country_code
    primary_key = get_primary_key(spark, schema_name)
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )

    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )

    staging = DeltaTable.forName(spark, staging_table_name).alias("staging").toDF()

    df_failed = staging.filter(
        ~staging[primary_key].isin(passing_row_ids["approved_rows"])
    )

    schema_reference = get_schema_columns_datahub(
        spark.spark_session, config.metastore_schema
    )

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
) -> sql.DataFrame:
    passing_row_ids = adls_file_client.download_json(config.filepath)

    schema_name = config.metastore_schema
    schema_columns = get_schema_columns(spark, schema_name)
    country_code = config.filename_components.country_code
    primary_key = get_primary_key(spark, schema_name)
    silver_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.SILVER
    )
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )

    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )

    staging = DeltaTable.forName(spark, staging_table_name).alias("staging")
    df_passed = staging.filter(
        staging[primary_key].isin(passing_row_ids["approved_rows"])
    )

    if spark.catalog.tableExists(silver_table_name):
        silver_dt = DeltaTable.forName(spark, silver_table_name)

        df_passed = add_missing_columns(df_passed, schema_columns)
        df_passed = transform_types(df_passed, schema_name, context)
        df_passed = compute_row_hash(df_passed)
        update_columns = [c.name for c in schema_columns if c.name != primary_key]
        query = build_deduped_merge_query(
            silver_dt, df_passed, primary_key, update_columns
        )

        if query is not None:
            execute_query_with_error_handler(
                spark, query, silver_tier_schema_name, country_code, context
            )
    else:
        df_passed.write.format("delta").mode("append").saveAsTable(silver_table_name)

    schema_reference = get_schema_columns_datahub(
        spark.spark_session, config.metastore_schema
    )

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    yield Output(
        None,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_passed),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def master(
    context: OpExecutionContext,
    silver: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    schema_name = config.metastore_schema
    schema_columns = get_schema_columns(spark, schema_name)
    country_code = config.filename_components.country_code
    primary_key = get_primary_key(spark, schema_name)
    gold_tier_schema_name = construct_schema_name_for_tier(schema_name, DataTier.GOLD)
    silver_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.SILVER
    )

    gold_table_name = construct_full_table_name(gold_tier_schema_name, country_code)
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)

    silver = DeltaTable.forName(spark, silver_table_name).alias("silver").toDF()

    if spark.catalog.tableExists(gold_table_name):
        gold_dt = DeltaTable.forName(spark, gold_table_name)

        silver = add_missing_columns(silver, schema_columns)
        silver = transform_types(silver, schema_name, context)
        silver = compute_row_hash(silver)
        silver = silver.select(get_schema_columns_master(spark, schema_name))
        update_columns = [c.name for c in schema_columns if c.name != primary_key]
        query = build_deduped_merge_query(gold_dt, silver, primary_key, update_columns)

        if query is not None:
            execute_query_with_error_handler(
                spark, query, gold_tier_schema_name, country_code, context
            )
    else:
        silver.write.format("delta").mode("append").saveAsTable(gold_table_name)

    schema_reference = get_schema_columns_datahub(
        spark.spark_session, config.metastore_schema
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )
    yield Output(
        None,
        metadata={**get_output_metadata(config), "preview": get_table_preview(silver)},
    )  ## @QUESTION: Not sure what to put here if it's inplace write in delta


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def reference(
    context: OpExecutionContext,
    silver: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    schema_name = config.metastore_schema
    schema_columns = get_schema_columns(spark, schema_name)
    country_code = config.filename_components.country_code
    primary_key = get_primary_key(spark, schema_name)
    gold_tier_schema_name = construct_schema_name_for_tier(schema_name, DataTier.GOLD)
    silver_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.SILVER
    )

    gold_table_name = construct_full_table_name(gold_tier_schema_name, country_code)
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)

    silver = DeltaTable.forName(spark, silver_table_name).alias("silver").toDF()

    if spark.catalog.tableExists(gold_table_name):
        gold_dt = DeltaTable.forName(spark, gold_table_name)

        silver = add_missing_columns(silver, schema_columns)
        silver = transform_types(silver, schema_name, context)
        silver = compute_row_hash(silver)
        silver = silver.select(get_schema_columns_reference(spark, schema_name))
        update_columns = [c.name for c in schema_columns if c.name != primary_key]
        query = build_deduped_merge_query(gold_dt, silver, primary_key, update_columns)

        if query is not None:
            execute_query_with_error_handler(
                spark, query, gold_tier_schema_name, country_code, context
            )
    else:
        silver.write.format("delta").mode("append").saveAsTable(gold_table_name)

    schema_reference = get_schema_columns_datahub(
        spark.spark_session, config.metastore_schema
    )
    datahub_emit_metadata_with_exception_catcher(
        context,
        schema_reference=schema_reference,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )

    yield Output(
        None,
        metadata={**get_output_metadata(config), "preview": get_table_preview(silver)},
    )  ## @QUESTION: Not sure what to put here if it's inplace write in delta
