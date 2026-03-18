from dagster_pyspark import PySparkResource
from datahub.specific.dataset import DatasetPatchBuilder
from delta.tables import DeltaTable
from models.approval_requests import ApprovalRequest
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)
from sqlalchemy import select, update
from src.constants import DataTier
from src.internal.common_assets.master_release_notes import send_master_release_notes
from src.internal.merge import (
    core_merge_logic,
    full_in_cluster_merge,
    partial_in_cluster_merge,
)
from src.resources import ResourceKey
from src.settings import DeploymentEnvironment, settings
from src.spark.transform_functions import (
    add_missing_columns,
    get_country_rt_schools,
    merge_connectivity_to_master,
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
from src.utils.sentry import capture_op_exceptions
from src.utils.spark import compute_row_hash, transform_types

from dagster import OpExecutionContext, Output, asset

# Pending-changes status constants (mirror staging.py)
_STATUS_APPROVED = "APPROVED"
_STATUS_REJECTED = "REJECTED"
_STATUS_PROCESSED = "PROCESSED"

# Pending-changes change_type constants (mirror staging.py)
_CHANGE_INSERT = "INSERT"
_CHANGE_UPDATE = "UPDATE"
_CHANGE_DELETE = "DELETE"


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value, deps=["silver"])
@capture_op_exceptions
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
@capture_op_exceptions
def manual_review_failed_rows(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session

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

    # Read REJECTED rows from pending_changes (staging) Delta table
    s.catalog.refreshTable(staging_table_name)
    staging_df = DeltaTable.forName(s, staging_table_name).toDF()
    rejected_rows = staging_df.filter(f.col("status") == _STATUS_REJECTED).select(
        *column_names
    )

    if check_table_exists(s, schema_name, country_code, DataTier.MANUAL_REJECTED):
        s.catalog.refreshTable(rejected_table_name)
        current_rejected = DeltaTable.forName(s, rejected_table_name).toDF()
        current_rejected = add_missing_columns(current_rejected, schema_columns)
        new_rejected = partial_in_cluster_merge(
            current_rejected, rejected_rows, primary_key, column_names
        )
    else:
        new_rejected = rejected_rows

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
@capture_op_exceptions
def silver(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session

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

    # Read APPROVED rows from the pending_changes (staging) Delta table
    s.catalog.refreshTable(staging_table_name)
    staging_df = DeltaTable.forName(s, staging_table_name).toDF()
    approved = staging_df.filter(f.col("status") == _STATUS_APPROVED)

    if approved.isEmpty():
        context.log.info(
            "No APPROVED rows in staging table. Returning current silver unchanged."
        )
        if check_table_exists(s, schema_name, country_code, DataTier.SILVER):
            s.catalog.refreshTable(silver_table_name)
            current_silver = DeltaTable.forName(s, silver_table_name).toDF()
            return Output(
                current_silver,
                metadata={
                    **get_output_metadata(config),
                    "preview": get_table_preview(current_silver),
                    "row_count": current_silver.count(),
                },
            )
        empty = s.createDataFrame([], StructType(schema_columns))
        return Output(
            empty,
            metadata={
                **get_output_metadata(config),
                "preview": get_table_preview(empty),
                "row_count": 0,
            },
        )

    inserts = approved.filter(f.col("change_type") == _CHANGE_INSERT).select(
        *column_names
    )
    updates = approved.filter(f.col("change_type") == _CHANGE_UPDATE).select(
        *column_names
    )
    deletes = approved.filter(f.col("change_type") == _CHANGE_DELETE).select(
        *column_names
    )

    insert_count = inserts.count()
    update_count = updates.count()
    delete_count = deletes.count()
    context.log.info(
        f"Approved rows: {insert_count} inserts, {update_count} updates, "
        f"{delete_count} deletes"
    )

    delete_ids = [row[primary_key] for row in deletes.select(primary_key).collect()]

    if check_table_exists(s, schema_name, country_code, DataTier.SILVER):
        s.catalog.refreshTable(silver_table_name)
        current_silver = DeltaTable.forName(s, silver_table_name).toDF()
        current_silver = add_missing_columns(current_silver, schema_columns)
        new_silver = core_merge_logic(
            current_silver,
            inserts,
            updates,
            deletes,
            primary_key,
            column_names,
            update_join_type="left",
        )

        # Verify deletes were applied
        if delete_ids:
            remaining = new_silver.filter(new_silver[primary_key].isin(delete_ids))
            remaining_count = remaining.count()
            if remaining_count > 0:
                context.log.error(
                    f"Delete verification failed: {remaining_count} of {len(delete_ids)} "
                    f"approved deletes still in silver."
                )
                from dagster import DagsterExecutionInterruptError

                raise DagsterExecutionInterruptError(
                    f"Deletes not applied: {remaining_count} rows still in silver"
                )
            context.log.info(
                f"Delete verification passed: all {len(delete_ids)} deletes removed."
            )
    else:
        new_silver = inserts

    if new_silver.isEmpty():
        context.log.info("Silver is empty after merge.")
        new_silver = s.createDataFrame([], StructType(schema_columns))

    new_silver = compute_row_hash(new_silver)

    # Mark approved rows as PROCESSED in the staging table
    DeltaTable.forName(s, staging_table_name).update(
        condition=f.col("status") == _STATUS_APPROVED,
        set={"status": f.lit(_STATUS_PROCESSED)},
    )
    context.log.info("Marked approved staging rows as PROCESSED.")

    # Reset ApprovalRequest state now that merge is complete
    formatted_dataset = f"School {config.dataset_type.capitalize()}"
    with get_db_context() as db:
        try:
            with db.begin():
                db.execute(
                    update(ApprovalRequest)
                    .where(
                        (ApprovalRequest.country == country_code)
                        & (ApprovalRequest.dataset == formatted_dataset)
                    )
                    .values(
                        {
                            ApprovalRequest.merge_requested: False,
                            ApprovalRequest.merge_requested_at: None,
                            ApprovalRequest.is_merge_processing: False,
                            ApprovalRequest.enabled: False,
                        }
                    )
                )
        except Exception as e:
            context.log.error(
                f"Failed to reset ApprovalRequest for {country_code} - "
                f"{formatted_dataset}: {e}"
            )

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
            "insert_count": insert_count,
            "update_count": update_count,
            "delete_count": delete_count,
        },
    )


@asset(deps=["manual_review_passed_rows", "manual_review_failed_rows"])
@capture_op_exceptions
def reset_staging_table(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
) -> None:
    """
    No-op for the geolocation pipeline: the pending_changes staging table is a
    persistent history log and does not need to be reset between merge cycles.

    For other pipelines (coverage) this asset still performs the silver-clone reset.
    """
    if config.dataset_type == "geolocation":
        context.log.info(
            "Geolocation uses the pending_changes staging design; skipping reset."
        )
        return

    from azure.core.exceptions import ResourceNotFoundError

    from src.utils.adls import ADLSFileClient

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

    formatted_dataset = f"School {config.dataset_type.capitalize()}"
    with get_db_context() as db:
        current_request = db.scalar(
            select(ApprovalRequest).where(
                (ApprovalRequest.country == country_code)
                & (ApprovalRequest.dataset == formatted_dataset)
            )
        )

        if current_request is None:
            context.log.warning(
                f"No ApprovalRequest found for {country_code} - {formatted_dataset}. "
                "Proceeding with reset."
            )
        elif current_request.enabled:
            context.log.warning(
                f"Reset blocked: ApprovalRequest is enabled for {country_code} - "
                f"{formatted_dataset}."
            )
            return
        elif current_request.is_merge_processing:
            context.log.warning(
                f"Reset blocked: merge still processing for {country_code} - "
                f"{formatted_dataset}."
            )
            return

    # Lazily import ADLSFileClient to avoid initialising it for the geolocation no-op path
    adls_file_client = ADLSFileClient()
    s.sql(f"DROP TABLE IF EXISTS {staging_table_name}")
    try:
        adls_file_client.delete(staging_table_path, is_directory=True)
    except ResourceNotFoundError as e:
        context.log.warning(e)

    schema_columns = get_schema_columns(s, config.metastore_schema)
    s.catalog.refreshTable(silver_table_name)
    silver_df = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    create_schema(s, staging_tier_schema_name)
    create_delta_table(
        s,
        staging_tier_schema_name,
        country_code,
        schema_columns,
        context,
        if_not_exists=True,
    )
    silver_df.write.format("delta").mode("append").saveAsTable(staging_table_name)

    with get_db_context() as db:
        try:
            with db.begin():
                result = db.execute(
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
                if result.rowcount == 0:
                    context.log.warning(
                        f"No ApprovalRequest found for {country_code} - "
                        f"{formatted_dataset}."
                    )
        except Exception as e:
            context.log.error(
                f"Failed to update ApprovalRequest for {country_code} - "
                f"{formatted_dataset}: {e}"
            )
            raise


def _handle_null_columns(schema_columns, primary_key, silver_columns):
    """Handle null columns by providing default values based on data type.

    If the column value is NULL, add a placeholder value if the following
    conditions are met:
    - The column is not nullable
    - The column is not the primary key
    - The column is not in the silver table

    Default values by type:
    - String: "Unknown"
    - Numeric (Int/Long/Double/Float): 0
    - Boolean: False
    - Timestamp: current_timestamp()
    """
    column_actions = {}
    for col in schema_columns:
        if (
            not col.nullable
            and col.name != primary_key
            and col.name not in [c.name for c in silver_columns]
        ):
            if col.dataType == StringType():
                column_actions[col.name] = f.coalesce(f.col(col.name), f.lit("Unknown"))
            elif isinstance(
                col.dataType, IntegerType | LongType | DoubleType | FloatType
            ):
                column_actions[col.name] = f.coalesce(f.col(col.name), f.lit(0))
            elif isinstance(col.dataType, BooleanType):
                column_actions[col.name] = f.coalesce(f.col(col.name), f.lit(False))
            elif isinstance(col.dataType, TimestampType):
                column_actions[col.name] = f.coalesce(
                    f.col(col.name), f.current_timestamp()
                )
    return column_actions


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value, deps=["silver"])
@capture_op_exceptions
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

    s.catalog.refreshTable(silver_table_name)
    silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    silver_columns = get_schema_columns(s, f"school_{config.dataset_type}")

    schema_columns = get_schema_columns(s, schema_name)
    column_names = [c.name for c in schema_columns]
    primary_key = get_primary_key(s, schema_name)

    # Conform to master schema and fill in missing values with NULL
    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, schema_name, context)
    silver = silver.select([c.name for c in schema_columns])

    master_table_name = construct_full_table_name("school_master", country_code)
    if check_table_exists(s, schema_name, country_code, DataTier.GOLD):
        s.catalog.refreshTable(master_table_name)
        current_master = DeltaTable.forName(s, master_table_name).toDF()
        current_master = add_missing_columns(current_master, schema_columns)
        new_master = full_in_cluster_merge(
            current_master, silver, primary_key, column_names
        )
    else:
        new_master = silver

    # Merge RT connectivity data into master (geolocation pipeline only).
    # RT data updates independently of geolocation uploads, so it belongs here
    # rather than in geolocation_bronze.  Pass both govt columns as uploaded_columns
    # to trigger the full connectivity derivation in merge_connectivity_to_master.
    if config.dataset_type == "geolocation":
        if settings.DEPLOY_ENV != DeploymentEnvironment.LOCAL:
            connectivity = get_country_rt_schools(s, country_code)
            new_master = merge_connectivity_to_master(
                new_master,
                connectivity,
                uploaded_columns=["connectivity_govt", "download_speed_govt"],
                mode="update",
            )
        else:
            context.log.info(
                "Skipping RT connectivity merge on local — "
                "pipeline_tables.school_connectivity_realtime_schools unavailable."
            )

    column_actions = _handle_null_columns(schema_columns, primary_key, silver_columns)
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
@capture_op_exceptions
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

    s.catalog.refreshTable(silver_table_name)
    silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    schema_columns = get_schema_columns(s, schema_name)
    column_names = [c.name for c in schema_columns]
    primary_key = get_primary_key(s, schema_name)
    silver_columns = silver.schema.fields

    silver = add_missing_columns(silver, schema_columns)
    silver = transform_types(silver, schema_name, context)
    silver = silver.select([c.name for c in schema_columns])

    reference_table_name = construct_full_table_name("school_reference", country_code)
    if check_table_exists(s, schema_name, country_code, DataTier.GOLD):
        s.catalog.refreshTable(reference_table_name)
        current_reference = DeltaTable.forName(s, reference_table_name).toDF()
        current_reference = add_missing_columns(current_reference, schema_columns)
        new_reference = full_in_cluster_merge(
            current_reference, silver, primary_key, column_names
        )
    else:
        new_reference = silver

    column_actions = _handle_null_columns(schema_columns, primary_key, silver_columns)
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
@capture_op_exceptions
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
