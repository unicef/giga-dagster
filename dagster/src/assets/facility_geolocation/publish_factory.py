from dagster import AssetIn, AssetKey, OpExecutionContext, Output, asset
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.types import StructType

from src.assets.common.assets import (
    _approval_reset_values,
    _update_approval_request,
)
from src.constants import StagingChangeType, StagingStatus
from src.facilities import (
    FacilityGeolocationSpec,
    merge_approved_facilities,
    refresh_facility_signature,
    split_approved_facilities,
    validate_facility_frames,
)
from src.internal.merge import core_merge_logic
from src.resources import ResourceKey
from src.spark.transform_functions import add_missing_columns
from src.utils.adls import ADLSFileClient
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import construct_full_table_name, get_schema_columns
from src.utils.spark import transform_types


def _approval_parts(payload: dict) -> tuple[str, list, list, str | None]:
    upload_id = payload.get("upload_id", "")
    approved_ids = payload.get("approved_change_ids", [])
    rejected_ids = payload.get("rejected_change_ids", [])
    approval_log_id = payload.get("approval_request_log_id")
    if not upload_id or not approval_log_id:
        raise ValueError(
            "Approval payload requires upload_id and approval_request_log_id"
        )
    if not isinstance(approved_ids, list) or not isinstance(rejected_ids, list):
        raise ValueError("Approved and rejected change IDs must be lists")
    if len(approved_ids) != len(set(approved_ids)) or len(rejected_ids) != len(
        set(rejected_ids)
    ):
        raise ValueError("Approval payload change IDs must be unique")
    if "__all__" in approved_ids and approved_ids != ["__all__"]:
        raise ValueError("__all__ cannot be mixed with explicit approved change IDs")
    if "__all__" in rejected_ids and rejected_ids != ["__all__"]:
        raise ValueError("__all__ cannot be mixed with explicit rejected change IDs")
    if set(approved_ids).intersection(rejected_ids):
        raise ValueError("Approved and rejected change IDs must be disjoint")
    return upload_id, approved_ids, rejected_ids, approval_log_id


def _resolve_ids(rows: sql.DataFrame, ids: list) -> sql.DataFrame:
    if ids == ["__all__"]:
        return rows
    if not ids:
        return rows.limit(0)
    return rows.filter(f.col("change_id").isin(ids))


def _validate_ids(rows: sql.DataFrame, ids: list) -> None:
    if not ids or ids == ["__all__"]:
        return
    found = {
        row["change_id"]
        for row in rows.filter(f.col("change_id").isin(ids))
        .select("change_id")
        .collect()
    }
    missing = set(ids).difference(found)
    if missing:
        raise ValueError(
            f"Approval payload contains unknown change IDs: {sorted(missing)}"
        )


def _load_or_empty(
    spark,
    table_name: str,
    schema,
) -> sql.DataFrame:
    if spark.catalog.tableExists(table_name):
        spark.catalog.refreshTable(table_name)
        return DeltaTable.forName(spark, table_name).toDF()
    return spark.createDataFrame([], schema)


def _merge_combined_silver(
    current: sql.DataFrame,
    approved: sql.DataFrame,
    spec: FacilityGeolocationSpec,
    schema_columns: list,
) -> sql.DataFrame:
    names = [field.name for field in schema_columns]
    current = add_missing_columns(current, schema_columns).select(*names)

    def operation_rows(change_type: StagingChangeType) -> sql.DataFrame:
        rows = approved.filter(f.col("change_type") == change_type)
        return add_missing_columns(rows, schema_columns).select(*names)

    inserts = operation_rows(StagingChangeType.INSERT)
    updates = operation_rows(StagingChangeType.UPDATE)
    deletes = operation_rows(StagingChangeType.DELETE)
    return core_merge_logic(
        current,
        inserts,
        updates,
        deletes,
        spec.giga_id_column,
        names,
        update_join_type="left",
    )


def build_facility_publish_assets(  # noqa: C901
    spec: FacilityGeolocationSpec,
) -> list:
    """Create approval and dual-table publish assets for a facility profile."""
    prefix = spec.assets.raw.removesuffix("_raw")
    approved_name = spec.assets.approved_rows
    rejected_name = spec.assets.rejected_rows
    completion_name = f"{prefix}_publish_complete"

    @asset(
        name=approved_name,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_SPARK_IO_MANAGER.value,
    )
    def approved_facility_rows(
        context: OpExecutionContext,
        config: FileConfig,
        spark: PySparkResource,
        adls_file_client: ADLSFileClient,
    ) -> Output[sql.DataFrame]:
        payload = adls_file_client.download_json(config.filepath)
        upload_id, approved_ids, _, _ = _approval_parts(payload)
        staging_table = construct_full_table_name(
            spec.tables.staging_schema,
            config.country_code,
        )
        spark.spark_session.catalog.refreshTable(staging_table)
        staging = DeltaTable.forName(spark.spark_session, staging_table).toDF()
        pending = staging.filter(
            (f.col("upload_id") == upload_id)
            & (f.col("change_type") != StagingChangeType.UNCHANGED)
            & (f.col("status") == StagingStatus.PENDING)
        )
        _validate_ids(pending, approved_ids)
        approved = _resolve_ids(pending, approved_ids).localCheckpoint()
        return Output(
            approved,
            metadata={
                **get_output_metadata(config),
                "row_count": approved.count(),
                "preview": get_table_preview(approved),
            },
        )

    @asset(
        name=rejected_name,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value,
        ins={"approved": AssetIn(key=AssetKey(approved_name))},
    )
    def rejected_facility_rows(
        context: OpExecutionContext,
        approved: sql.DataFrame,
        config: FileConfig,
        spark: PySparkResource,
        adls_file_client: ADLSFileClient,
    ) -> Output[sql.DataFrame]:
        del approved
        payload = adls_file_client.download_json(config.filepath)
        upload_id, _, rejected_ids, _ = _approval_parts(payload)
        staging_table = construct_full_table_name(
            spec.tables.staging_schema,
            config.country_code,
        )
        staging = DeltaTable.forName(spark.spark_session, staging_table).toDF()
        upload_rows = staging.filter(
            (f.col("upload_id") == upload_id)
            & (f.col("change_type") != StagingChangeType.UNCHANGED)
        )
        _validate_ids(upload_rows, rejected_ids)
        pending = upload_rows.filter(f.col("status") == StagingStatus.PENDING)
        rejected = _resolve_ids(pending, rejected_ids)
        if rejected_ids:
            condition = (
                (f.col("upload_id") == upload_id)
                & (f.col("status") == StagingStatus.PENDING)
            )
            if rejected_ids != ["__all__"]:
                condition &= f.col("change_id").isin(rejected_ids)
            DeltaTable.forName(spark.spark_session, staging_table).update(
                condition=condition,
                set={"status": f.lit(StagingStatus.REJECTED)},
            )
        schema_columns = [
            field.name
            for field in get_schema_columns(
                spark.spark_session,
                spec.metastore_schema,
            )
        ]
        rejected = rejected.select(
            *[column for column in schema_columns if column in rejected.columns]
        )
        return Output(
            rejected,
            metadata={
                **get_output_metadata(config),
                "row_count": rejected.count(),
                "preview": get_table_preview(rejected),
            },
        )

    @asset(
        name=spec.assets.silver,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value,
        ins={"approved": AssetIn(key=AssetKey(approved_name))},
    )
    def facility_silver(
        approved: sql.DataFrame,
        config: FileConfig,
        spark: PySparkResource,
    ) -> Output[sql.DataFrame]:
        schema_columns = get_schema_columns(
            spark.spark_session,
            spec.metastore_schema,
        )
        silver_table = construct_full_table_name(
            spec.tables.silver_schema,
            config.country_code,
        )
        current = _load_or_empty(
            spark.spark_session,
            silver_table,
            StructType(schema_columns),
        )
        published = _merge_combined_silver(
            current,
            approved,
            spec,
            schema_columns,
        )
        return Output(
            published,
            metadata={
                **get_output_metadata(config),
                "row_count": published.count(),
            },
        )

    @asset(
        name=spec.assets.core,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value,
        ins={
            "approved": AssetIn(key=AssetKey(approved_name)),
            "published_silver": AssetIn(key=AssetKey(spec.assets.silver)),
        },
    )
    def facility_core(
        approved: sql.DataFrame,
        published_silver: sql.DataFrame,
        config: FileConfig,
        spark: PySparkResource,
    ) -> Output[sql.DataFrame]:
        del published_silver
        projected = split_approved_facilities(approved, spec, allow_missing=True)
        core_table = construct_full_table_name(
            spec.tables.core_schema,
            config.country_code,
        )
        extension_table = construct_full_table_name(
            spec.tables.extension_schema,
            config.country_code,
        )
        current_core = _load_or_empty(
            spark.spark_session,
            core_table,
            projected.core.schema,
        )
        current_extension = _load_or_empty(
            spark.spark_session,
            extension_table,
            projected.extension.schema,
        )
        result = merge_approved_facilities(
            current_core,
            current_extension,
            approved,
            spec,
        )
        published = refresh_facility_signature(
            transform_types(
                result.core,
                spec.tables.core_schema,
            ),
            "core_signature",
        )
        return Output(
            published,
            metadata={
                **get_output_metadata(config),
                "row_count": published.count(),
                "insert_count": result.insert_count,
                "update_count": result.update_count,
                "delete_count": result.delete_count,
            },
        )

    @asset(
        name=spec.assets.extension,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value,
        ins={
            "approved": AssetIn(key=AssetKey(approved_name)),
            "published_core": AssetIn(key=AssetKey(spec.assets.core)),
        },
    )
    def facility_extension(
        approved: sql.DataFrame,
        published_core: sql.DataFrame,
        config: FileConfig,
        spark: PySparkResource,
    ) -> Output[sql.DataFrame]:
        projected = split_approved_facilities(approved, spec, allow_missing=True)
        extension_table = construct_full_table_name(
            spec.tables.extension_schema,
            config.country_code,
        )
        current_extension = _load_or_empty(
            spark.spark_session,
            extension_table,
            projected.extension.schema,
        )
        # The core output has already incorporated this approval. Recompute only the
        # extension side against its current table and then validate the pair.
        result = merge_approved_facilities(
            published_core,
            current_extension,
            approved,
            spec,
        )
        published_extension = refresh_facility_signature(
            transform_types(
                result.extension,
                spec.tables.extension_schema,
            ),
            "extension_signature",
        )
        facility_core_rows = published_core.filter(
            f.col("facility_type") == spec.facility_type
        )
        validate_facility_frames(
            facility_core_rows,
            published_extension,
            spec,
        )
        return Output(
            published_extension,
            metadata={
                **get_output_metadata(config),
                "row_count": published_extension.count(),
            },
        )

    @asset(
        name=completion_name,
        group_name=spec.group_name,
        ins={
            "silver": AssetIn(key=AssetKey(spec.assets.silver)),
            "core": AssetIn(key=AssetKey(spec.assets.core)),
            "extension": AssetIn(key=AssetKey(spec.assets.extension)),
            "rejected": AssetIn(key=AssetKey(rejected_name)),
        },
    )
    def facility_publish_complete(
        context: OpExecutionContext,
        silver: sql.DataFrame,
        core: sql.DataFrame,
        extension: sql.DataFrame,
        rejected: sql.DataFrame,
        config: FileConfig,
        spark: PySparkResource,
        adls_file_client: ADLSFileClient,
    ) -> Output[None]:
        del silver, core, extension, rejected
        payload = adls_file_client.download_json(config.filepath)
        upload_id, approved_ids, _, approval_log_id = _approval_parts(payload)
        staging_table = construct_full_table_name(
            spec.tables.staging_schema,
            config.country_code,
        )
        if approved_ids == ["__all__"]:
            approved_condition = (
                (f.col("upload_id") == upload_id)
                & (f.col("change_type") != StagingChangeType.UNCHANGED)
                & (f.col("status") == StagingStatus.PENDING)
            )
        elif approved_ids:
            approved_condition = (
                (f.col("upload_id") == upload_id)
                & f.col("change_id").isin(approved_ids)
                & (f.col("status") == StagingStatus.PENDING)
            )
        else:
            approved_condition = f.lit(False)
        table = DeltaTable.forName(spark.spark_session, staging_table)
        table.update(
            condition=approved_condition,
            set={
                "status": f.lit(StagingStatus.PROCESSED),
                "processed_at": f.current_timestamp(),
                "approval_request_log_id": f.lit(approval_log_id),
            },
        )
        _update_approval_request(
            context,
            config.country_code,
            spec.approval_label,
            _approval_reset_values(spark.spark_session, staging_table),
        )
        table.update(
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
            "Facility core and extension writes succeeded; staging rows processed."
        )
        return Output(None, metadata=get_output_metadata(config))

    return [
        approved_facility_rows,
        rejected_facility_rows,
        facility_silver,
        facility_core,
        facility_extension,
        facility_publish_complete,
    ]
