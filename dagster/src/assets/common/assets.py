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
from src.constants import DataTier, StagingChangeType, StagingStatus
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
from src.utils.delta import check_table_exists, create_schema
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

    # Read PENDING non-UNCHANGED rows for this upload from the staging table
    s.catalog.refreshTable(staging_table_name)
    staging_df = DeltaTable.forName(s, staging_table_name).toDF()
    upload_rows = staging_df.filter(
        (f.col("upload_id") == upload_id)
        & (f.col("change_type") != StagingChangeType.UNCHANGED)
        & (f.col("status") == StagingStatus.PENDING)
    )

    rejected_rows = _resolve_change_ids(
        upload_rows, rejected_change_ids, s.sparkContext
    ).select(*column_names)

    # Mark rejected rows as REJECTED in the staging table
    if rejected_change_ids == [_APPROVE_ALL]:
        reject_condition = (
            (f.col("upload_id") == upload_id)
            & (f.col("change_type") != StagingChangeType.UNCHANGED)
            & (f.col("status") == StagingStatus.PENDING)
        )
    elif not rejected_change_ids:
        reject_condition = f.lit(False)
    else:
        reject_condition = (
            (f.col("upload_id") == upload_id)
            & f.col("change_id").isin(rejected_change_ids)
            & (f.col("status") == StagingStatus.PENDING)
        )
    DeltaTable.forName(s, staging_table_name).update(
        condition=reject_condition,
        set={"status": f.lit(StagingStatus.REJECTED)},
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
        (f.col("upload_id") == upload_id)
        & (f.col("change_type") != StagingChangeType.UNCHANGED)
    ).count()
    matched_pending = staging_df.filter(
        (f.col("upload_id") == upload_id)
        & (f.col("change_type") != StagingChangeType.UNCHANGED)
        & (f.col("status") == StagingStatus.PENDING)
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
            & (f.col("change_type") != StagingChangeType.UNCHANGED)
            & (f.col("status") == StagingStatus.PENDING)
        )
    if not approved_change_ids:
        return f.lit(False)
    return (
        (f.col("upload_id") == upload_id)
        & f.col("change_id").isin(approved_change_ids)
        & (f.col("status") == StagingStatus.PENDING)
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
            "status": f.lit(StagingStatus.PROCESSED),
            "processed_at": f.current_timestamp(),
            "approval_request_log_id": f.lit(approval_request_log_id),
        },
    )
    DeltaTable.forName(spark, staging_table_name).update(
        condition=(f.col("upload_id") == upload_id)
        & (f.col("change_type") == StagingChangeType.UNCHANGED)
        & (f.col("status") == StagingStatus.PENDING),
        set={
            "status": f.lit(StagingStatus.PROCESSED_UNCHANGED),
            "processed_at": f.current_timestamp(),
            "approval_request_log_id": f.lit(approval_request_log_id),
        },
    )
    context.log.info("Marked approved staging rows as PROCESSED / PROCESSED_UNCHANGED.")

    remaining_pending = (
        DeltaTable.forName(spark, staging_table_name)
        .toDF()
        .filter(
            (f.col("status") == StagingStatus.PENDING)
            & (f.col("change_type") != StagingChangeType.UNCHANGED)
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


def _cascade_deletes_to_coverage_silver(
    spark: SparkSession,
    context: OpExecutionContext,
    country_code: str,
    primary_key: str,
    delete_ids: list[str],
) -> None:
    coverage_silver_tier_schema = construct_schema_name_for_tier(
        "school_coverage", DataTier.SILVER
    )
    coverage_silver_table = construct_full_table_name(
        coverage_silver_tier_schema, country_code
    )
    if check_table_exists(spark, "school_coverage", country_code, DataTier.SILVER):
        spark.catalog.refreshTable(coverage_silver_table)
        DeltaTable.forName(spark, coverage_silver_table).delete(
            f.col(primary_key).isin(delete_ids)
        )
        context.log.info(
            f"Cascaded {len(delete_ids)} deletes to coverage silver for {country_code}."
        )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
@capture_op_exceptions
def silver(  # noqa: C901
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

    # Read PENDING non-UNCHANGED rows for this upload from the staging table
    s.catalog.refreshTable(staging_table_name)
    staging_df = DeltaTable.forName(s, staging_table_name).toDF()

    _log_staging_diagnostics(context, staging_df, upload_id)

    upload_rows = staging_df.filter(
        (f.col("upload_id") == upload_id)
        & (f.col("change_type") != StagingChangeType.UNCHANGED)
        & (f.col("status") == StagingStatus.PENDING)
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

    inserts = approved.filter(f.col("change_type") == StagingChangeType.INSERT).select(
        *column_names
    )
    updates = approved.filter(f.col("change_type") == StagingChangeType.UPDATE).select(
        *column_names
    )
    deletes = approved.filter(f.col("change_type") == StagingChangeType.DELETE).select(
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

        if config.dataset_type == "geolocation":
            _cascade_deletes_to_coverage_silver(
                s, context, country_code, primary_key, delete_ids
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
    config: FileConfig,
) -> None:
    country_code = config.country_code
    formatted_dataset = f"School {config.dataset_type.capitalize()}"

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
        s, f"school_{config.dataset_type}", country_code, DataTier.STAGING
    ):
        staging_tier_schema_name = construct_schema_name_for_tier(
            f"school_{config.dataset_type}", DataTier.STAGING
        )
        staging_table_name = construct_full_table_name(
            staging_tier_schema_name, country_code
        )
        approval_data = adls_file_client.download_json(config.filepath)
        upload_id, approved_ids, _, approval_log_id = _parse_approval_file(
            approval_data
        )
        processed_condition = _build_processed_condition(approved_ids, upload_id)
        DeltaTable.forName(s, staging_table_name).update(
            condition=processed_condition,
            set={
                "status": f.lit(StagingStatus.PROCESSED),
                "processed_at": f.current_timestamp(),
                "approval_request_log_id": f.lit(approval_log_id),
            },
        )
        DeltaTable.forName(s, staging_table_name).update(
            condition=(f.col("upload_id") == upload_id)
            & (f.col("change_type") == StagingChangeType.UNCHANGED)
            & (f.col("status") == StagingStatus.PENDING),
            set={
                "status": f.lit(StagingStatus.PROCESSED_UNCHANGED),
                "processed_at": f.current_timestamp(),
                "approval_request_log_id": f.lit(approval_log_id),
            },
        )
        context.log.info(
            "Marked approved staging rows as PROCESSED / PROCESSED_UNCHANGED "
            "(silver confirmed written)."
        )

        remaining_pending = (
            DeltaTable.forName(s, staging_table_name)
            .toDF()
            .filter(
                (f.col("status") == StagingStatus.PENDING)
                & (f.col("change_type") != StagingChangeType.UNCHANGED)
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
        if config.dataset_type == "coverage":
            # Coverage is not authoritative for which schools exist — geolocation is.
            # Keep every row already in master; only update coverage columns for schools
            # present in school_coverage_silver (coalesce preserves existing master values
            # for schools absent from coverage silver). Never insert or delete.
            column_names_no_pk = [c for c in column_names if c != primary_key]
            new_master = (
                current_master.alias("master")
                .join(silver.alias("silver"), primary_key, "left")
                .withColumns(
                    {
                        c: f.coalesce(f.col(f"silver.{c}"), f.col(f"master.{c}"))
                        for c in column_names_no_pk
                    }
                )
                .select(*column_names)
            )
        else:
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
        if config.dataset_type == "coverage":
            column_names_no_pk = [c for c in column_names if c != primary_key]
            new_reference = (
                current_reference.alias("reference")
                .join(silver.alias("silver"), primary_key, "left")
                .withColumns(
                    {
                        c: f.coalesce(f.col(f"silver.{c}"), f.col(f"reference.{c}"))
                        for c in column_names_no_pk
                    }
                )
                .select(*column_names)
            )
        else:
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
    """Backfill master_version on PROCESSED rows in the staging table that don't have one yet.

    Called from broadcast_master_release_notes after the master Delta table has been
    written, so the version read from history is the one that includes the current run.
    Only runs for the geolocation pipeline (the only pipeline using the staging table).
    """
    if config.dataset_type != "geolocation":
        return

    staging_tier_schema_name = construct_schema_name_for_tier(
        f"school_{config.dataset_type}", DataTier.STAGING
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
        condition=f.col("status").isin(
            StagingStatus.PROCESSED, StagingStatus.PROCESSED_UNCHANGED
        )
        & f.col("master_version").isNull(),
        set={"master_version": f.lit(master_version)},
    )
    context.log.info(
        f"Stamped master_version={master_version} on PROCESSED staging rows."
    )


@asset(deps=["master"])
@capture_op_exceptions
def dq_kit_post_approval(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    adls_file_client: ADLSFileClient,
) -> Output[None]:
    """Regenerate the DQ Kit ZIP after approval, including a school_master CSV only for geolocation.

    Writes two artefacts directly via ``ADLSFileClient``
    1. ``…/school-geolocation/master-export/<country>/<stem>.csv`` — full
       ``school_master.<country>`` snapshot, taken right after the merge.
    2. ``…/school-geolocation/<country>/dq-kit/DQ_Kit_<country>_geolocation_<stem>.zip``
        - Overriting the pre-approval kit that was generated by the portal when the file was uploaded.
    """
    import io as _io
    from pathlib import Path

    from models.file_upload import FileUpload
    from src.constants import constants
    from src.utils.dq_kit_generator import generate_dq_kit_zip_bytes

    if config.dataset_type != "geolocation":
        context.log.info(
            f"dq_kit_post_approval is a no-op for dataset_type="
            f"{config.dataset_type!r}"
        )
        return Output(None, metadata={**get_output_metadata(config), "skipped": True})

    s: SparkSession = spark.spark_session
    country_code = config.country_code

    # Recover upload_id from the approval JSON (filename has no id).
    approval_data = adls_file_client.download_json(config.filepath)
    upload_id, _, _, _ = _parse_approval_file(approval_data)
    if not upload_id:
        raise ValueError(
            f"Approval file {config.filepath!r} has no upload_id; cannot regenerate kit"
        )

    with get_db_context() as db:
        file_upload = db.scalar(select(FileUpload).where(FileUpload.id == upload_id))
        if file_upload is None:
            raise FileNotFoundError(
                f"FileUpload with id `{upload_id}` not found; cannot regenerate DQ kit"
            )
        original_filename = file_upload.original_filename
        raw_filename = file_upload.filename

    stem = Path(raw_filename).stem
    dataset_prefix = f"school-{config.dataset_type}"
    dq_root = f"{constants.dq_results_folder}/{dataset_prefix}"
    master_csv_path = f"{dq_root}/master-export/{country_code}/{stem}.csv"
    kit_zip_path = (
        f"{dq_root}/dq-kit/{country_code}/"
        f"DQ_Kit_{country_code}_{config.dataset_type}_{stem}.zip"
    )

    # 1. Export school_master snapshot as a single CSV.
    master_table_name = construct_full_table_name("school_master", country_code)
    s.catalog.refreshTable(master_table_name)
    master_pdf = DeltaTable.forName(s, master_table_name).toDF().toPandas()

    csv_buffer = _io.StringIO()
    master_pdf.to_csv(csv_buffer, index=False)
    ADLSFileClient.upload_raw(
        None, csv_buffer.getvalue().encode("utf-8"), master_csv_path
    )
    context.log.info(
        f"Wrote master export ({len(master_pdf)} rows) to {master_csv_path}"
    )

    # 2. Regenerate the kit zip — generator picks up the new master CSV via the
    #    convention-based path and overwrites the pre-approval kit in place.
    zip_bytes, kit_filename = generate_dq_kit_zip_bytes(
        country_code=country_code,
        upload_id=upload_id,
        dataset=config.dataset_type,
        original_filename=original_filename,
        stem=stem,
        adls_client=adls_file_client,
        context=context,
    )
    ADLSFileClient.upload_raw(None, zip_bytes, kit_zip_path)
    context.log.info(f"Overwrote DQ Kit at {kit_zip_path} ({len(zip_bytes)} bytes)")

    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "master_csv_path": master_csv_path,
            "kit_zip_path": kit_zip_path,
            "kit_filename": kit_filename,
            "kit_size_bytes": len(zip_bytes),
            "master_row_count": len(master_pdf),
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
