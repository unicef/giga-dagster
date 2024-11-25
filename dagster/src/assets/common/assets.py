from country_converter import CountryConverter
from dagster_pyspark import PySparkResource
from datahub.specific.dataset import DatasetPatchBuilder
from delta.tables import DeltaTable
from models.approval_requests import ApprovalRequest
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StringType
from sqlalchemy import update
from src.constants import DataTier
from src.internal.common_assets.master_release_notes import send_master_release_notes
from src.internal.merge import (
    full_in_cluster_merge,
    manual_review_dedupe_strat,
    partial_cdf_in_cluster_merge,
)
from src.resources import ResourceKey
from src.settings import DeploymentEnvironment, settings
from src.spark.transform_functions import (
    add_missing_columns,
    connectivity_rt_dataset,
    merge_connectivity_to_master,
    standardize_connectivity_type,
)
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.datahub.emitter import get_rest_emitter
from src.utils.db.primary import get_db_context
from src.utils.delta import check_table_exists, create_delta_table, create_schema
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
    column_names = [c.name for c in schema_columns]
    primary_key = get_primary_key(s, schema_name)
    staging_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.STAGING
    )
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, country_code
    )
    rejected_tier_schema_name = construct_schema_name_for_tier(
        schema_name, DataTier.MANUAL_REJECTED
    )
    rejected_table_name = construct_full_table_name(
        rejected_tier_schema_name, country_code
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
        ),
    )

    if len(passing_rows_change_ids) == 0:
        df_failed = staging_cdf
    elif len(passing_rows_change_ids) == 1 and passing_rows_change_ids[0] == "__all__":
        df_failed = staging_cdf.limit(0)
    else:
        bc_passing_rows_change_ids = s.sparkContext.broadcast(passing_rows_change_ids)
        df_failed = staging_cdf.filter(
            ~f.col("change_id").isin(bc_passing_rows_change_ids.value)
        )

    # Check if a rejects table already exists
    # If yes, do an in-cluster merge
    # Else, the current df_failed is the initial rejects table
    if check_table_exists(s, schema_name, country_code, DataTier.MANUAL_REJECTED):
        rejected = DeltaTable.forName(s, rejected_table_name).toDF()
        rejected = add_missing_columns(rejected, schema_columns)
        new_rejected = partial_cdf_in_cluster_merge(
            rejected, df_failed, column_names, primary_key, context
        )
    else:
        new_rejected = df_failed.select(*column_names)

    schema_reference = get_schema_columns_datahub(s, schema_name)
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    return Output(
        new_rejected,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(new_rejected),
            "row_count": new_rejected.count(),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def silver(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    passing_rows_change_ids = adls_file_client.download_json(config.filepath)
    context.log.info(f"{len(passing_rows_change_ids)=}")

    schema_name = config.metastore_schema
    country_code = config.country_code
    schema_columns = get_schema_columns(s, schema_name)
    column_names = [c.name for c in schema_columns]
    primary_key = get_primary_key(s, schema_name)
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
        ),
    )

    if len(passing_rows_change_ids) == 0:
        df_passed = staging_cdf.limit(0)
    elif len(passing_rows_change_ids) == 1 and passing_rows_change_ids[0] == "__all__":
        df_passed = staging_cdf
    else:
        bc_passing_rows_change_ids = s.sparkContext.broadcast(passing_rows_change_ids)
        df_passed = staging_cdf.filter(
            f.col("change_id").isin(bc_passing_rows_change_ids.value)
        )
    context.log.info(f"{df_passed.count()=}")

    df_passed = manual_review_dedupe_strat(df_passed)

    if check_table_exists(s, schema_name, country_code, DataTier.SILVER):
        current_silver = DeltaTable.forName(s, silver_table_name).toDF()
        current_silver = add_missing_columns(current_silver, schema_columns)
        new_silver = partial_cdf_in_cluster_merge(
            current_silver, df_passed, column_names, primary_key, context
        )
    else:
        new_silver = df_passed

    schema_reference = get_schema_columns_datahub(s, schema_name)
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    return Output(
        new_silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(new_silver),
            "row_count": new_silver.count(),
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
    silver_columns = get_schema_columns(s, f"school_{config.dataset_type}")

    schema_columns = get_schema_columns(s, schema_name)
    column_names = [c.name for c in schema_columns]
    primary_key = get_primary_key(s, schema_name)

    if settings.DEPLOY_ENV != DeploymentEnvironment.LOCAL:
        raw_connectivity_columns = {"download_speed_govt", "connectivity_govt"}
        if raw_connectivity_columns.issubset(set(silver.columns)):
            # QoS Columns
            coco = CountryConverter()
            country_code_2 = coco.convert(country_code, to="ISO2")
            connectivity = connectivity_rt_dataset(s, country_code_2)
            silver = merge_connectivity_to_master(silver, connectivity)

    # standardize the connectivity type
    if "connectivity_type_govt" in silver.columns:
        silver = standardize_connectivity_type(silver)

    # Conform to master schema and fill in missing values with NULL
    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, schema_name, context)
    silver = silver.select([c.name for c in schema_columns])

    if check_table_exists(s, schema_name, country_code, DataTier.GOLD):
        current_master = DeltaTable.forName(
            s, construct_full_table_name("school_master", country_code)
        ).toDF()
        current_master = add_missing_columns(current_master, schema_columns)
        new_master = full_in_cluster_merge(
            current_master, silver, primary_key, column_names
        )
    else:
        new_master = silver

    column_actions = {}
    for col in schema_columns:
        # If the column value is NULL, add a placeholder value if the following
        # conditions are met:
        # - The column is not nullable
        # - The column is not the primary key
        # - The column type is string
        # - The column is not in the silver table (e.g. the column comes from the
        #   coverage schema, but we are currently processing a silver geolocation table)
        if (
            not col.nullable
            and col.name != primary_key
            and col.dataType == StringType()
            and col.name not in [c.name for c in silver_columns]
        ):
            column_actions[col.name] = f.coalesce(f.col(col.name), f.lit("Unknown"))
    new_master = new_master.withColumns(column_actions)
    new_master = compute_row_hash(new_master)

    schema_reference = get_schema_columns_datahub(s, schema_name)
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    return Output(
        new_master,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(silver),
            "row_count": new_master.count(),
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
    column_names = [c.name for c in schema_columns]
    primary_key = get_primary_key(s, schema_name)
    silver_columns = silver.schema.fields

    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, schema_name, context)
    silver = silver.select([c.name for c in schema_columns])

    if check_table_exists(s, schema_name, country_code, DataTier.GOLD):
        current_reference = DeltaTable.forName(
            s, construct_full_table_name("school_reference", country_code)
        ).toDF()
        current_reference = add_missing_columns(current_reference, schema_columns)
        new_reference = full_in_cluster_merge(
            current_reference, silver, primary_key, column_names
        )
    else:
        new_reference = silver

    column_actions = {}
    for col in schema_columns:
        if (
            not col.nullable
            and col.name != primary_key
            and col.dataType == StringType()
            and col.name not in [c.name for c in silver_columns]
        ):
            column_actions[col.name] = f.coalesce(f.col(col.name), f.lit("Unknown"))
    new_reference = new_reference.withColumns(column_actions)
    new_reference = compute_row_hash(new_reference)

    schema_reference = get_schema_columns_datahub(s, schema_name)
    datahub_emit_metadata_with_exception_catcher(
        context,
        config=config,
        schema_reference=schema_reference,
        spark=spark,
    )

    return Output(
        new_reference,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(new_reference),
            "row_count": new_reference.count(),
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

    with get_rest_emitter() as emitter:
        context.log.info(f"{config.datahub_destination_dataset_urn=}")

        for patch_mcp in (
            DatasetPatchBuilder(config.datahub_destination_dataset_urn)
            .add_custom_properties(
                {
                    "Dataset Version": str(metadata["version"]),
                    "Row Count": f'{metadata["rows"]:,}',
                    "Rows Added": f'{metadata["added"]:,}',
                    "Rows Updated": f'{metadata["modified"]:,}',
                    "Rows Deleted": f'{metadata["deleted"]:,}',
                }
            )
            .build()
        ):
            try:
                emitter.emit(patch_mcp, lambda e, s: context.log.info(f"{e=}\n{s=}"))
            except Exception as e:
                context.log.error(str(e))

    return Output(None, metadata=metadata)
