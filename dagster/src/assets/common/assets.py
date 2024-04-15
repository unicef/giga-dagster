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
from src.utils.delta import (
    build_deduped_merge_query,
    create_delta_table,
    create_schema,
    execute_query_with_error_handler,
)
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_datahub_schema_columns,
    get_master_schema_columns,
    get_primary_key,
    get_reference_schema_columns,
    get_schema_columns,
)
from src.utils.spark import compute_row_hash, transform_types

from dagster import AssetIn, AssetKey, OpExecutionContext, Output, asset


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

    schema_reference = get_datahub_schema_columns(s, config.metastore_schema)

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
    schema_columns = get_schema_columns(s, schema_name)
    country_code = config.country_code
    primary_key = get_primary_key(s, schema_name)
    silver_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.SILVER
    )
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )

    context.log.info(
        f"SHEMA NAMES: {silver_tier_schema_name}, {staging_tier_schema_name}"
    )
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )

    context.log.info(f"TBL NAMES: {silver_table_name}, {staging_table_name}")
    staging = DeltaTable.forName(s, staging_table_name).alias("staging").toDF()
    context.log.info(f"stgprkey: {staging[primary_key]}")
    df_passed = staging.filter(staging[primary_key].isin(passing_row_ids))

    context.log.info(f"df: {df_passed.toPandas()}")
    if spark.spark_session.catalog.tableExists(silver_table_name):
        silver_dt = DeltaTable.forName(s, silver_table_name)

        df_passed = add_missing_columns(df_passed, schema_columns)
        df_passed = transform_types(df_passed, schema_name, context)
        df_passed = compute_row_hash(df_passed)
        update_columns = [c.name for c in schema_columns if c.name != primary_key]
        query = build_deduped_merge_query(
            silver_dt, df_passed, primary_key, update_columns
        )

        if query is not None:
            execute_query_with_error_handler(
                s, query, silver_tier_schema_name, country_code, context
            )
    else:
        create_schema(s, silver_tier_schema_name)
        create_delta_table(
            s,
            silver_tier_schema_name,
            country_code,
            schema_columns,
            context,
            if_not_exists=True,
        )
        df_passed.write.format("delta").mode("append").saveAsTable(silver_table_name)

    schema_reference = get_datahub_schema_columns(s, config.metastore_schema)

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


@asset(
    io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value,
    ins={"start_after_silver": AssetIn(key=AssetKey("silver"))},
)
def master(
    context: OpExecutionContext,
    start_after_silver,
    spark: PySparkResource,
    config: FileConfig,
):
    s: SparkSession = spark.spark_session
    schema_name = config.metastore_schema
    schema_columns = get_schema_columns(s, schema_name)
    country_code = config.country_code
    primary_key = get_primary_key(s, schema_name)
    gold_tier_schema_name = construct_schema_name_for_tier(schema_name, DataTier.GOLD)
    silver_tier_schema_name = construct_schema_name_for_tier(
        f"school_{config.dataset_type}", DataTier.SILVER
    )

    gold_table_name = construct_full_table_name(gold_tier_schema_name, country_code)
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)

    silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()

    if s.catalog.tableExists(gold_table_name):
        gold_dt = DeltaTable.forName(s, gold_table_name)

        silver = add_missing_columns(silver, schema_columns)
        silver = transform_types(silver, schema_name, context)
        silver = compute_row_hash(silver)
        silver = silver.select(get_master_schema_columns(s, schema_name))
        update_columns = [c.name for c in schema_columns if c.name != primary_key]
        query = build_deduped_merge_query(gold_dt, silver, primary_key, update_columns)

        if query is not None:
            execute_query_with_error_handler(
                s, query, gold_tier_schema_name, country_code, context
            )
    else:
        create_schema(s, gold_tier_schema_name)
        create_delta_table(
            s,
            gold_tier_schema_name,
            country_code,
            schema_columns,
            context,
            if_not_exists=True,
        )
        silver.write.format("delta").mode("append").saveAsTable(gold_table_name)

    schema_reference = get_datahub_schema_columns(s, config.metastore_schema)
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=s,
        schema_reference=schema_reference,
    )
    yield Output(
        None,
        metadata={**get_output_metadata(config), "preview": get_table_preview(silver)},
    )  ## @QUESTION: Not sure what to put here if it's inplace write in delta


@asset(
    io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value,
    ins={"start_after_silver": AssetIn(key=AssetKey("silver"))},
)
def reference(
    context: OpExecutionContext,
    start_after_silver,
    spark: PySparkResource,
    config: FileConfig,
):
    s: SparkSession = spark.spark_session
    schema_name = config.metastore_schema
    schema_columns = get_schema_columns(s, schema_name)
    country_code = config.country_code
    primary_key = get_primary_key(s, schema_name)
    gold_tier_schema_name = construct_schema_name_for_tier(schema_name, DataTier.GOLD)
    silver_tier_schema_name = construct_schema_name_for_tier(
        "school_{config.dataset_type}", DataTier.SILVER
    )

    gold_table_name = construct_full_table_name(gold_tier_schema_name, country_code)
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)

    silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()

    if s.catalog.tableExists(gold_table_name):
        gold_dt = DeltaTable.forName(s, gold_table_name)

        silver = add_missing_columns(silver, schema_columns)
        silver = transform_types(silver, schema_name, context)
        silver = compute_row_hash(silver)
        silver = silver.select(get_reference_schema_columns(s, schema_name))
        update_columns = [c.name for c in schema_columns if c.name != primary_key]
        query = build_deduped_merge_query(gold_dt, silver, primary_key, update_columns)

        if query is not None:
            execute_query_with_error_handler(
                s, query, gold_tier_schema_name, country_code, context
            )
    else:
        create_schema(s, gold_tier_schema_name)
        create_delta_table(
            s,
            gold_tier_schema_name,
            country_code,
            schema_columns,
            context,
            if_not_exists=True,
        )
        silver.write.format("delta").mode("append").saveAsTable(gold_table_name)

    schema_reference = get_datahub_schema_columns(s, config.metastore_schema)
    datahub_emit_metadata_with_exception_catcher(
        context,
        schema_reference=schema_reference,
        country_code=config.country_code,
        dataset_urn=config.datahub_destination_dataset_urn,
    )

    yield Output(
        None,
        metadata={**get_output_metadata(config), "preview": get_table_preview(silver)},
    )  ## @QUESTION: Not sure what to put here if it's inplace write in delta
