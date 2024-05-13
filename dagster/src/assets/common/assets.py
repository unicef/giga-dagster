from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from models.approval_requests import ApprovalRequest
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from sqlalchemy import update
from src.constants import DataTier
from src.internal.common_assets.master_release_notes import send_master_release_notes
from src.resources import ResourceKey
from src.spark.transform_functions import add_missing_columns
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db import get_db_context
from src.utils.delta import create_delta_table, create_schema
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

from azure.core.exceptions import ResourceNotFoundError
from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value, deps=["silver"])
def manual_review_passed_rows(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[None]:
    s: SparkSession = spark.spark_session

    schema_name = config.metastore_schema
    schema_reference = get_schema_columns_datahub(s, schema_name)

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )
    return Output(None)


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def manual_review_failed_rows(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    passing_rows_change_ids = adls_file_client.download_json(config.filepath)

    schema_name = config.metastore_schema
    country_code = config.country_code
    schema_columns = get_schema_columns(s, schema_name)
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )

    staging_cdf = (
        s.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table(staging_table_name)
    )
    staging_cdf = staging_cdf.withColumn(
        "change_id",
        f.concat_ws(
            "|",
            f.col("school_id_giga"),
            f.col("_change_type"),
            f.col("_commit_version").cast(StringType()),
            f.col("_commit_timestamp").cast(StringType()),
        ),
    )

    df_failed = staging_cdf.filter(~f.col("change_id").isin(passing_rows_change_ids))
    df_failed = df_failed.select(*[c.name for c in schema_columns])

    schema_reference = get_schema_columns_datahub(s, schema_name)

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )
    return Output(df_failed, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def silver(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    passing_rows_change_ids = adls_file_client.download_json(config.filepath)

    schema_name = config.metastore_schema
    country_code = config.country_code
    schema_columns = get_schema_columns(s, schema_name)
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )
    silver_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.SILVER
    )
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)
    primary_key = get_primary_key(s, schema_name)

    staging_cdf = (
        s.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table(staging_table_name)
    )
    staging_cdf = staging_cdf.withColumn(
        "change_id",
        f.concat_ws(
            "|",
            f.col("school_id_giga"),
            f.col("_change_type"),
            f.col("_commit_version").cast(StringType()),
            f.col("_commit_timestamp").cast(StringType()),
        ),
    )

    df_passed = staging_cdf.filter(f.col("change_id").isin(passing_rows_change_ids))

    # In case multiple rows with the same school_id_giga are present,
    # get only the row of the latest version.
    df_passed = df_passed.withColumn(
        "row_number",
        f.row_number().over(
            Window.partitionBy("school_id_giga").orderBy(
                f.col("_commit_version").desc(),
                f.col("_change_type"),
            )
        ),
    ).filter(f.col("row_number") == 1)

    silver = DeltaTable.forName(s, silver_table_name).toDF()

    inserts = df_passed.filter(df_passed["_change_type"] == "insert")
    inserts = inserts.select(*[c.name for c in schema_columns])
    updates = df_passed.filter(df_passed["_change_type"] == "update")
    updates = updates.select(*[c.name for c in schema_columns])
    deletes = df_passed.filter(df_passed["_change_type"] == "delete")
    deletes = deletes.select(*[c.name for c in schema_columns])

    silver = silver.unionAll(inserts)
    silver = silver.filter(~f.col(primary_key).isin(deletes.select(primary_key)))
    silver = updates.unionAll(silver).dropDuplicates([primary_key])

    schema_reference = get_schema_columns_datahub(s, schema_name)

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    return Output(
        silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_passed),
        },
    )


@asset(deps=["manual_review_passed_rows", "manual_review_failed_rows"])
def reset_staging_table(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    adls_file_client: ADLSFileClient,
) -> None:
    s: SparkSession = spark.spark_session
    country_code = config.country_code
    staging_tier_schema_name = construct_schema_name_for_tier(
        f"school_{config.dataset_type}", DataTier.STAGING
    )
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )
    staging_table_path = config.destination_filepath
    silver_tier_schema_name = construct_schema_name_for_tier(
        f"school_{config.dataset_type}", DataTier.SILVER
    )
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)

    s.sql(f"DROP TABLE IF EXISTS {staging_table_name}")

    try:
        adls_file_client.delete(staging_table_path, is_directory=True)
    except ResourceNotFoundError as e:
        context.log.warning(e)

    schema_columns = get_schema_columns(s, config.metastore_schema)
    silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    create_schema(s, staging_tier_schema_name)
    create_delta_table(
        s,
        staging_tier_schema_name,
        country_code,
        schema_columns,
        context,
        if_not_exists=True,
    )
    silver.write.format("delta").mode("append").saveAsTable(staging_table_name)

    formatted_dataset = f"School {config.dataset_type.capitalize()}"
    with get_db_context() as db:
        with db.begin():
            db.execute(
                update(ApprovalRequest)
                .where(
                    (ApprovalRequest.country == country_code)
                    & (ApprovalRequest.dataset == formatted_dataset)
                )
                .values(
                    {
                        ApprovalRequest.is_merge_processing: False,
                        ApprovalRequest.enabled: False,
                    }
                )
            )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value, deps=["silver"])
def master(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    schema_name = config.metastore_schema
    country_code = config.country_code
    silver_tier_schema_name = construct_schema_name_for_tier(
        f"school_{config.dataset_type}", DataTier.SILVER
    )
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)

    silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    silver_columns = silver.schema.fields

    schema_columns = get_schema_columns(s, schema_name)
    primary_key = get_primary_key(s, schema_name)

    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, schema_name, context)
    silver = silver.select([c.name for c in schema_columns])

    column_actions = {}
    for col in schema_columns:
        if (
            not col.nullable
            and col.name != primary_key
            and col.dataType == StringType()
            and col.name not in [c.name for c in silver_columns]
        ):
            column_actions[col.name] = f.when(
                f.col(col.name).isNull() | (f.col(col.name) == ""),
                f.lit("Unknown"),
            )

    silver = silver.withColumns(column_actions)
    silver = compute_row_hash(silver)

    schema_reference = get_schema_columns_datahub(s, schema_name)
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    return Output(
        silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(silver),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value, deps=["silver"])
def reference(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    schema_name = config.metastore_schema
    country_code = config.country_code
    silver_tier_schema_name = construct_schema_name_for_tier(
        f"school_{config.dataset_type}", DataTier.SILVER
    )
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)

    silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    schema_columns = get_schema_columns(s, schema_name)
    primary_key = get_primary_key(s, schema_name)
    silver_columns = silver.schema.fields

    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, schema_name, context)
    silver = silver.select([c.name for c in schema_columns])

    column_actions = {}
    for col in schema_columns:
        if (
            not col.nullable
            and col.name != primary_key
            and col.dataType == StringType()
            and col.name not in [c.name for c in silver_columns]
        ):
            column_actions[col.name] = f.when(
                f.col(col.name).isNull() | (f.col(col.name) == ""),
                f.lit("Unknown"),
            )

    silver = silver.withColumns(column_actions)
    silver = compute_row_hash(silver)

    schema_reference = get_schema_columns_datahub(s, schema_name)
    datahub_emit_metadata_with_exception_catcher(
        context,
        config=config,
        schema_reference=schema_reference,
        spark=spark,
    )

    return Output(
        silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(silver),
        },
    )


@asset
async def broadcast_master_release_notes(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    master: sql.DataFrame,
) -> Output[None]:
    metadata = await send_master_release_notes(context, config, spark, master)
    if metadata is None:
        return Output(None)

    return Output(None, metadata=metadata)
