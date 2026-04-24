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
from src.spark.transform_functions import (
    add_missing_columns,
)
from src.utils.adls import ADLSFileClient
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
_STATUS_PENDING = "PENDING"
_STATUS_APPROVED = "APPROVED"
_STATUS_REJECTED = "REJECTED"
_STATUS_PROCESSED = "PROCESSED"

# Pending-changes change_type constants (mirror staging.py)
_CHANGE_INSERT = "INSERT"
_CHANGE_UPDATE = "UPDATE"
_CHANGE_DELETE = "DELETE"
_CHANGE_UNCHANGED = "UNCHANGED"

# Approval file shorthands
_APPROVE_ALL = "__all__"


def _parse_approval_file(approval_data: dict) -> tuple[str, list, list, str | None]:
    return (
        approval_data.get("upload_id", ""),
        approval_data.get("approved_change_ids", []),
        approval_data.get("rejected_change_ids", []),
        approval_data.get("approval_request_log_id"),
    )


def _resolve_change_ids(
    upload_rows: sql.DataFrame,
    change_ids: list,
    spark_context,
) -> sql.DataFrame:
    """Filter upload_rows by change_ids.

    ["__all__"] → all rows
    []          → no rows
    [id, ...]   → filter to matching change_ids
    """
    if change_ids == [_APPROVE_ALL]:
        return upload_rows
    if not change_ids:
        return upload_rows.limit(0)
    bc = spark_context.broadcast(change_ids)
    return upload_rows.filter(f.col("change_id").isin(bc.value))


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
    adls_file_client: ADLSFileClient,
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

    # Download and parse the approval file written by the portal
    approval_data = adls_file_client.download_json(config.filepath)
    upload_id, _, rejected_change_ids, _ = _parse_approval_file(approval_data)
    context.log.info(
        f"[manual_review_failed_rows] approval file: upload_id={upload_id!r}, "
        f"rejected_change_ids={rejected_change_ids!r}"
    )

    # Read PENDING non-UNCHANGED rows for this upload from pending_changes
    s.catalog.refreshTable(staging_table_name)
    staging_df = DeltaTable.forName(s, staging_table_name).toDF()
    upload_rows = staging_df.filter(
        (f.col("upload_id") == upload_id)
        & (f.col("change_type") != _CHANGE_UNCHANGED)
        & (f.col("status") == _STATUS_PENDING)
    )

    rejected_rows = _resolve_change_ids(
        upload_rows, rejected_change_ids, s.sparkContext
    ).select(*column_names)

    # Mark rejected rows as REJECTED in pending_changes
    if rejected_change_ids == [_APPROVE_ALL]:
        reject_condition = (
            (f.col("upload_id") == upload_id)
            & (f.col("change_type") != _CHANGE_UNCHANGED)
            & (f.col("status") == _STATUS_PENDING)
        )
    elif not rejected_change_ids:
        reject_condition = f.lit(False)
    else:
        reject_condition = (
            (f.col("upload_id") == upload_id)
            & f.col("change_id").isin(rejected_change_ids)
            & (f.col("status") == _STATUS_PENDING)
        )
    DeltaTable.forName(s, staging_table_name).update(
        condition=reject_condition,
        set={"status": f.lit(_STATUS_REJECTED)},
    )
    context.log.info("Marked rejected staging rows as REJECTED.")

    create_schema(s, rejected_tier_schema_name)
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


def _log_staging_diagnostics(
    context: OpExecutionContext,
    staging_df: sql.DataFrame,
    upload_id: str,
) -> None:
    total_staging = staging_df.count()
    matched_upload = staging_df.filter(f.col("upload_id") == upload_id).count()
    matched_non_unchanged = staging_df.filter(
        (f.col("upload_id") == upload_id) & (f.col("change_type") != _CHANGE_UNCHANGED)
    ).count()
    matched_pending = staging_df.filter(
        (f.col("upload_id") == upload_id)
        & (f.col("change_type") != _CHANGE_UNCHANGED)
        & (f.col("status") == _STATUS_PENDING)
    ).count()
    context.log.info(
        f"[silver] staging table: total={total_staging}, "
        f"upload_id match={matched_upload}, "
        f"+non-unchanged={matched_non_unchanged}, "
        f"+status=PENDING={matched_pending}"
    )


def _build_processed_condition(approved_change_ids: list, upload_id: str):
    if approved_change_ids == [_APPROVE_ALL]:
        return (
            (f.col("upload_id") == upload_id)
            & (f.col("change_type") != _CHANGE_UNCHANGED)
            & (f.col("status") == _STATUS_PENDING)
        )
    if not approved_change_ids:
        return f.lit(False)
    return (
        (f.col("upload_id") == upload_id)
        & f.col("change_id").isin(approved_change_ids)
        & (f.col("status") == _STATUS_PENDING)
    )


def _stamp_processed_and_reset_approval(
    context: OpExecutionContext,
    spark: SparkSession,
    staging_table_name: str,
    approved_change_ids: list,
    upload_id: str,
    approval_request_log_id: str | None,
    country_code: str,
    formatted_dataset: str,
) -> None:
    """Stamp approved staging rows PROCESSED and reset the ApprovalRequest.

    Used by non-geolocation pipelines from the silver asset.  For geolocation this
    is deferred to the master asset so that a failed silver IO manager write cannot
    leave rows permanently marked PROCESSED with no corresponding silver data.
    """
    processed_condition = _build_processed_condition(approved_change_ids, upload_id)
    DeltaTable.forName(spark, staging_table_name).update(
        condition=processed_condition,
        set={
            "status": f.lit(_STATUS_PROCESSED),
            "processed_at": f.current_timestamp(),
            "approval_request_log_id": f.lit(approval_request_log_id),
        },
    )
    context.log.info("Marked approved staging rows as PROCESSED.")

    remaining_pending = (
        DeltaTable.forName(spark, staging_table_name)
        .toDF()
        .filter(
            (f.col("status") == _STATUS_PENDING)
            & (f.col("change_type") != _CHANGE_UNCHANGED)
        )
        .count()
    )
    update_values = {ApprovalRequest.is_merge_processing: False}
    if remaining_pending == 0:
        update_values[ApprovalRequest.enabled] = False

    with get_db_context() as db:
        try:
            with db.begin():
                db.execute(
                    update(ApprovalRequest)
                    .where(
                        (ApprovalRequest.country == country_code)
                        & (ApprovalRequest.dataset == formatted_dataset)
                    )
                    .values(update_values)
                )
        except Exception as e:
            context.log.error(
                f"Failed to reset ApprovalRequest for {country_code} - "
                f"{formatted_dataset}: {e}"
            )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
@capture_op_exceptions
def silver(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    adls_file_client: ADLSFileClient,
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

    # Download and parse the approval file written by the portal
    approval_data = adls_file_client.download_json(config.filepath)
    upload_id, approved_change_ids, _, approval_request_log_id = _parse_approval_file(
        approval_data
    )
    context.log.info(
        f"[silver] approval file: upload_id={upload_id!r}, "
        f"approved_change_ids={approved_change_ids!r}, "
        f"approval_request_log_id={approval_request_log_id!r}"
    )

    # Read PENDING non-UNCHANGED rows for this upload from pending_changes
    s.catalog.refreshTable(staging_table_name)
    staging_df = DeltaTable.forName(s, staging_table_name).toDF()

    _log_staging_diagnostics(context, staging_df, upload_id)

    upload_rows = staging_df.filter(
        (f.col("upload_id") == upload_id)
        & (f.col("change_type") != _CHANGE_UNCHANGED)
        & (f.col("status") == _STATUS_PENDING)
    )

    approved = _resolve_change_ids(upload_rows, approved_change_ids, s.sparkContext)

    # Break the lazy lineage back to the staging Delta table immediately.
    # Delta Lake invalidates Spark's DataFrame cache when the table is written to
    # (e.g. our PENDING→PROCESSED update), so .cache() is insufficient.
    # localCheckpoint() materialises the rows into executor memory and severs the
    # dependency on staging, ensuring the IO manager's isEmpty() call cannot
    # accidentally re-read the now-PROCESSED rows and find 0 results.
    approved = approved.localCheckpoint()

    if approved.isEmpty():
        context.log.info(
            "No approved rows for this upload. Returning current silver unchanged."
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

    formatted_dataset = f"School {config.dataset_type.capitalize()}"

    if config.dataset_type == "geolocation":
        # For geolocation the PROCESSED stamp is deferred to the master asset, which
        # runs after the IO manager has confirmed the silver write succeeded.  Stamping
        # here would mark rows PROCESSED even if the silver write then fails, leaving
        # them orphaned.  Only clear is_merge_processing now; the remaining-pending
        # check and enabled=False reset happen in master once the stamp is applied.
        with get_db_context() as db:
            try:
                with db.begin():
                    db.execute(
                        update(ApprovalRequest)
                        .where(
                            (ApprovalRequest.country == country_code)
                            & (ApprovalRequest.dataset == formatted_dataset)
                        )
                        .values({ApprovalRequest.is_merge_processing: False})
                    )
            except Exception as e:
                context.log.error(
                    f"Failed to reset ApprovalRequest for {country_code} - "
                    f"{formatted_dataset}: {e}"
                )
    else:
        # Non-geolocation pipelines: stamp PROCESSED immediately.  Their staging
        # table is transient (reset after every merge cycle) so the atomicity
        # guarantee is less critical.
        _stamp_processed_and_reset_approval(
            context,
            s,
            staging_table_name,
            approved_change_ids,
            upload_id,
            approval_request_log_id,
            country_code,
            formatted_dataset,
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

    from src.utils.adls import ADLSFileClient

    from azure.core.exceptions import ResourceNotFoundError

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


def _handle_null_columns(schema_columns, primary_key):
    """Handle null columns by providing default values based on data type.

    If the column value is NULL, add a placeholder value if the following
    conditions are met:
    - The column is not nullable
    - The column is not the primary key

    Default values by type:
    - String: "Unknown"
    - Numeric (Int/Long/Double/Float): 0
    - Boolean: False
    - Timestamp: current_timestamp()
    """
    column_actions = {}
    for col in schema_columns:
        if not col.nullable and col.name != primary_key:
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
    adls_file_client: ADLSFileClient,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    schema_name = config.metastore_schema
    country_code = config.country_code

    # For geolocation: stamp PROCESSED now that the silver IO manager write has
    # succeeded.  The stamp was intentionally deferred from the silver asset so that
    # a failed silver write cannot leave staging rows permanently marked PROCESSED
    # with no corresponding silver data.
    if config.dataset_type == "geolocation" and check_table_exists(
        s, schema_name, country_code, DataTier.STAGING
    ):
        staging_tier_schema_name = construct_schema_name_for_tier(
            schema_name, DataTier.STAGING
        )
        staging_table_name = construct_full_table_name(
            staging_tier_schema_name, country_code
        )
        approval_data = adls_file_client.download_json(config.filepath)
        upload_id_geo, approved_ids_geo, _, approval_log_id_geo = _parse_approval_file(
            approval_data
        )
        processed_condition = _build_processed_condition(
            approved_ids_geo, upload_id_geo
        )
        DeltaTable.forName(s, staging_table_name).update(
            condition=processed_condition,
            set={
                "status": f.lit(_STATUS_PROCESSED),
                "processed_at": f.current_timestamp(),
                "approval_request_log_id": f.lit(approval_log_id_geo),
            },
        )
        context.log.info(
            "Marked approved staging rows as PROCESSED (silver confirmed written)."
        )

        remaining_pending = (
            DeltaTable.forName(s, staging_table_name)
            .toDF()
            .filter(
                (f.col("status") == _STATUS_PENDING)
                & (f.col("change_type") != _CHANGE_UNCHANGED)
            )
            .count()
        )
        formatted_dataset = f"School {config.dataset_type.capitalize()}"
        approval_update_values: dict = {ApprovalRequest.is_merge_processing: False}
        if remaining_pending == 0:
            approval_update_values[ApprovalRequest.enabled] = False
        with get_db_context() as db:
            try:
                with db.begin():
                    db.execute(
                        update(ApprovalRequest)
                        .where(
                            (ApprovalRequest.country == country_code)
                            & (ApprovalRequest.dataset == formatted_dataset)
                        )
                        .values(approval_update_values)
                    )
            except Exception as e:
                context.log.error(
                    f"Failed to reset ApprovalRequest for {country_code} - "
                    f"{formatted_dataset}: {e}"
                )

    silver_tier_schema_name = construct_schema_name_for_tier(
        f"school_{config.dataset_type}", DataTier.SILVER
    )
    silver_table_name = construct_full_table_name(silver_tier_schema_name, country_code)

    s.catalog.refreshTable(silver_table_name)
    silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
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

    column_actions = _handle_null_columns(schema_columns, primary_key)
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

    column_actions = _handle_null_columns(schema_columns, primary_key)
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


def _stamp_master_version(
    context: OpExecutionContext,
    spark: SparkSession,
    config: FileConfig,
) -> None:
    """Backfill master_version on PROCESSED pending_changes rows that don't have one yet.

    Called from broadcast_master_release_notes after the master Delta table has been
    written, so the version read from history is the one that includes the current run.
    Only runs for the geolocation pipeline (the only pipeline using pending_changes).
    """
    if config.dataset_type != "geolocation":
        return

    staging_tier_schema_name = construct_schema_name_for_tier(
        config.metastore_schema, DataTier.STAGING
    )
    staging_table_name = construct_full_table_name(
        staging_tier_schema_name, config.country_code
    )
    if not spark.catalog.tableExists(staging_table_name):
        context.log.info("No staging table found; skipping master_version stamp.")
        return

    master_table_name = construct_full_table_name("school_master", config.country_code)
    master_dt = DeltaTable.forName(spark, master_table_name)
    master_version = (
        master_dt.history().orderBy(f.col("version").desc()).first().version
    )
    if master_version is None:
        context.log.warning("Could not determine master Delta version; skipping stamp.")
        return

    DeltaTable.forName(spark, staging_table_name).update(
        condition=(f.col("status") == _STATUS_PROCESSED)
        & f.col("master_version").isNull(),
        set={"master_version": f.lit(master_version)},
    )
    context.log.info(
        f"Stamped master_version={master_version} on PROCESSED staging rows."
    )


@asset
@capture_op_exceptions
async def broadcast_master_release_notes(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    master: sql.DataFrame,
) -> Output[None]:
    s: SparkSession = spark.spark_session
    _stamp_master_version(context, s, config)

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
