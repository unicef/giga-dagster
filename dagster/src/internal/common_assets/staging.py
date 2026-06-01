import enum

from delta import DeltaTable
from models.approval_requests import ApprovalRequest
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    TimestampType,
)
from sqlalchemy import select, update

from dagster import OpExecutionContext
from src.constants import DataTier, StagingChangeType, StagingStatus
from src.schemas.file_upload import FileUploadConfig
from src.spark.transform_functions import add_missing_columns
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_lineage import emit_lineage_base
from src.utils.db.primary import get_db_context
from src.utils.delta import (
    check_table_exists,
    create_delta_table,
    create_schema,
)
from src.utils.op_config import FileConfig
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_primary_key,
    get_schema_columns,
)
from src.utils.spark import compute_row_hash, transform_types


def get_files_for_review(
    adls_file_client: ADLSFileClient,
    config: FileConfig,
    skip_current_file: bool = False,
):
    files_for_review = []
    for file_info in adls_file_client.list_paths(
        str(config.filepath_object.parent), recursive=False
    ):
        if skip_current_file and file_info.name == config.filepath:
            continue
        files_for_review.append(file_info)

    files_for_review = sorted(files_for_review, key=lambda p: p.last_modified)
    return files_for_review


class StagingMode(enum.Enum):
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class StagingStep:
    def __init__(
        self,
        context: OpExecutionContext,
        config: FileConfig,
        adls_file_client: ADLSFileClient,
        spark: SparkSession,
        change_type: StagingMode,
    ):
        self.context = context
        self.config = config
        self.adls_file_client = adls_file_client
        self.spark = spark
        self.change_type = change_type

        self.schema_name = config.metastore_schema
        self.country_code = config.country_code
        self.schema_columns = get_schema_columns(spark, self.schema_name)
        self.primary_key = get_primary_key(spark, self.schema_name)
        self.silver_tier_schema_name = construct_schema_name_for_tier(
            self.schema_name,
            DataTier.SILVER,
        )
        self.staging_tier_schema_name = construct_schema_name_for_tier(
            self.schema_name,
            DataTier.STAGING,
        )
        self.silver_table_name = construct_full_table_name(
            self.silver_tier_schema_name, self.country_code
        )
        self.staging_table_name = construct_full_table_name(
            self.staging_tier_schema_name,
            self.country_code,
        )

    def __call__(self, upstream_df: sql.DataFrame | list[str]) -> sql.DataFrame | None:
        if self.change_type == StagingMode.DELETE:
            pending = self._build_delete_records(upstream_df)
        else:
            pending = self._build_upsert_records(upstream_df)

        if pending is None or pending.isEmpty():
            return None

        self._write_pending_records(pending)
        self._update_approval_request_status()
        self._stamp_file_upload_staging_complete()
        self._emit_lineage()

        return pending

    def _build_upsert_records(self, df: sql.DataFrame) -> sql.DataFrame | None:
        """Build staging rows for an upsert (INSERT/UPDATE/UNCHANGED)."""
        uploaded_columns = self._get_uploaded_columns()
        df = self._prepare_df(df)
        schema_col_names = [c.name for c in self.schema_columns]
        upload_id = self.config.filename_components.id

        if not self.silver_table_exists:
            # No silver table yet — every row is an INSERT
            df = compute_row_hash(df)
            df = self._select_schema_cols(df)
            df = (
                df.withColumn("change_type", f.lit(StagingChangeType.INSERT))
                .withColumn("upload_id", f.lit(upload_id))
                .withColumn(
                    "uploaded_columns",
                    f.array(*[f.lit(c) for c in uploaded_columns]),
                )
                .withColumn("status", f.lit(StagingStatus.PENDING))
                .withColumn(
                    "change_id",
                    f.concat_ws(
                        "|",
                        f.col(self.primary_key),
                        f.lit(upload_id),
                        f.col("change_type"),
                    ),
                )
                .withColumn("created_at", f.current_timestamp())
                .withColumn("processed_at", f.lit(None).cast(TimestampType()))
            )
            self.context.log.info(f"No silver table; all {df.count()} rows are INSERT")
            return df

        # Silver exists: left-join and fill non-uploaded cols from silver
        silver_df = DeltaTable.forName(self.spark, self.silver_table_name).toDF()

        # Prefix all silver columns to avoid name conflicts
        silver_prefixed = silver_df.select(
            *[f.col(c).alias(f"_s_{c}") for c in silver_df.columns]
        )
        joined = df.join(
            silver_prefixed,
            df[self.primary_key] == silver_prefixed[f"_s_{self.primary_key}"],
            "left",
        )

        # For every schema column: use the Bronze value when it is non-null;
        # otherwise fall back to the Silver value for existing rows.
        # This handles uploaded columns, columns derived from uploaded columns
        # (e.g. education_level from education_level_govt, admin columns from
        # lat/lon), and columns absent from the file — all with one rule.
        row_in_silver = f.col(f"_s_{self.primary_key}").isNotNull()
        for col_name in schema_col_names:
            s_col = f"_s_{col_name}"
            if s_col not in joined.columns:
                continue
            joined = joined.withColumn(
                col_name,
                f.when(
                    row_in_silver & f.col(col_name).isNull(), f.col(s_col)
                ).otherwise(f.col(col_name)),
            )

        # Drop all silver-prefixed columns (including _s_signature)
        s_cols_to_drop = [c for c in joined.columns if c.startswith("_s_")]
        joined = joined.drop(*s_cols_to_drop)

        # Compute hash over the fully-merged row (same column set as when silver was written)
        joined = compute_row_hash(joined)

        # Join with silver signatures to determine INSERT / UPDATE / UNCHANGED
        # (must happen before _select_schema_cols so that 'signature' is still present)
        silver_sigs = silver_df.select(
            f.col(self.primary_key).alias("_sig_pk"),
            f.col("signature").alias("_silver_sig"),
        )
        joined = joined.join(
            silver_sigs,
            joined[self.primary_key] == f.col("_sig_pk"),
            "left",
        )
        joined = joined.withColumn(
            "change_type",
            f.when(f.col("_sig_pk").isNull(), f.lit(StagingChangeType.INSERT))
            .when(
                f.col("signature") == f.col("_silver_sig"),
                f.lit(StagingChangeType.UNCHANGED),
            )
            .otherwise(f.lit(StagingChangeType.UPDATE)),
        )
        joined = joined.drop("_sig_pk", "_silver_sig")

        # Trim to schema columns + change_type before persisting.
        # Inline the select instead of calling _select_schema_cols so that
        # change_type (not a schema column) is preserved alongside them.
        available = [c for c in schema_col_names if c in joined.columns]
        joined = joined.select(*available, "change_type")

        joined = (
            joined.withColumn("upload_id", f.lit(upload_id))
            .withColumn(
                "uploaded_columns",
                f.array(*[f.lit(c) for c in uploaded_columns]),
            )
            .withColumn("status", f.lit(StagingStatus.PENDING))
            .withColumn(
                "change_id",
                f.concat_ws(
                    "|", f.col(self.primary_key), f.lit(upload_id), f.col("change_type")
                ),
            )
            .withColumn("created_at", f.current_timestamp())
            .withColumn("processed_at", f.lit(None).cast(TimestampType()))
        )
        return joined

    def _build_delete_records(self, delete_ids: list[str]) -> sql.DataFrame | None:
        """Build staging rows for a DELETE operation."""
        if not self.silver_table_exists:
            self.context.log.warning(
                "Silver table does not exist; cannot stage DELETE records."
            )
            return None

        silver_df = DeltaTable.forName(self.spark, self.silver_table_name).toDF()
        rows = silver_df.filter(f.col(self.primary_key).isin(delete_ids))

        if rows.isEmpty():
            self.context.log.warning(
                f"None of {len(delete_ids)} delete IDs found in silver. Skipping."
            )
            return None

        upload_id = self.config.filename_components.id
        rows = (
            rows.withColumn("change_type", f.lit(StagingChangeType.DELETE))
            .withColumn("upload_id", f.lit(upload_id))
            .withColumn("uploaded_columns", f.array(f.lit(self.primary_key)))
            .withColumn("status", f.lit(StagingStatus.PENDING))
            .withColumn(
                "change_id",
                f.concat_ws(
                    "|", f.col(self.primary_key), f.lit(upload_id), f.col("change_type")
                ),
            )
            .withColumn("created_at", f.current_timestamp())
            .withColumn("processed_at", f.lit(None).cast(TimestampType()))
        )
        return rows

    def _write_pending_records(self, pending: sql.DataFrame) -> None:
        """Append rows to the staging Delta table."""
        create_schema(self.spark, self.staging_tier_schema_name)

        pending_extra_fields = [
            StructField("upload_id", StringType(), nullable=False),
            StructField("change_type", StringType(), nullable=False),
            StructField("uploaded_columns", ArrayType(StringType()), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("change_id", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("processed_at", TimestampType(), nullable=True),
            StructField("approval_request_log_id", StringType(), nullable=True),
            StructField("master_version", LongType(), nullable=True),
        ]
        pending_schema = list(self.schema_columns) + pending_extra_fields

        if not self.spark.catalog.tableExists(self.staging_table_name):
            create_delta_table(
                self.spark,
                self.staging_tier_schema_name,
                self.country_code,
                pending_schema,
                self.context,
                replace=True,
                partition_by=["upload_id"],
            )
        else:
            upload_id = self.config.filename_components.id
            DeltaTable.forName(self.spark, self.staging_table_name).delete(
                f.col("upload_id") == upload_id
            )
            self.context.log.info(
                f"Deleted existing staging rows for upload_id={upload_id}"
            )

        # Cast pending columns to the expected schema types before writing
        schema_type_map = {field.name: field.dataType for field in pending_schema}
        pending = pending.withColumns(
            {
                col: f.col(col).cast(dtype)
                for col, dtype in schema_type_map.items()
                if col in pending.columns
            }
        )

        (
            pending.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(self.staging_table_name)
        )

    def _update_approval_request_status(self) -> None:
        """Enable the ApprovalRequest if any actionable (non-UNCHANGED) rows exist."""
        actionable = (
            DeltaTable.forName(self.spark, self.staging_table_name)
            .toDF()
            .filter(
                (f.col("status") == StagingStatus.PENDING)
                & (f.col("change_type") != StagingChangeType.UNCHANGED)
            )
            .count()
        )
        if actionable == 0:
            self.context.log.info(
                "No actionable changes in staging table (only UNCHANGED). "
                "Skipping ApprovalRequest update."
            )
            return

        formatted_dataset = f"School {self.config.dataset_type.capitalize()}"
        with get_db_context() as db:
            try:
                with db.begin():
                    current_request = self._get_current_approval_request(
                        db, formatted_dataset
                    )
                    if current_request is None:
                        self.context.log.warning(
                            f"No ApprovalRequest found for "
                            f"{self.country_code} - {formatted_dataset}"
                        )
                        return

                    if current_request.enabled:
                        self.context.log.info(
                            f"ApprovalRequest already enabled for "
                            f"{self.country_code} - {formatted_dataset}. Skipping."
                        )
                        return

                    result = db.execute(
                        update(ApprovalRequest)
                        .where(
                            (ApprovalRequest.country == self.country_code)
                            & (ApprovalRequest.dataset == formatted_dataset)
                            & (~ApprovalRequest.enabled)
                        )
                        .values(
                            {
                                ApprovalRequest.enabled: True,
                                ApprovalRequest.is_merge_processing: False,
                            }
                        ),
                    )
                    if result.rowcount > 0:
                        self.context.log.info(
                            f"Successfully set enabled=True for "
                            f"{self.country_code} - {formatted_dataset}"
                        )
                    else:
                        self.context.log.info(
                            "No rows updated (already enabled or state changed)."
                        )
            except Exception as e:
                self.context.log.error(
                    f"Failed to update ApprovalRequest for "
                    f"{self.country_code} - {formatted_dataset}: {e}"
                )

    def _stamp_file_upload_staging_complete(self) -> None:
        """Set is_processed_in_staging=True and approval_status='PENDING' when enabled."""
        upload_id = self.config.filename_components.id
        formatted_dataset = f"School {self.config.dataset_type.capitalize()}"
        try:
            with get_db_context() as db:
                with db.begin():
                    file_upload = db.scalar(
                        select(FileUpload).where(FileUpload.id == upload_id)
                    )
                    if file_upload is None:
                        self.context.log.warning(
                            f"FileUpload with id `{upload_id}` not found; "
                            "cannot stamp is_processed_in_staging."
                        )
                        return

                    approval_request = db.scalar(
                        select(ApprovalRequest).where(
                            (ApprovalRequest.country == self.country_code)
                            & (ApprovalRequest.dataset == formatted_dataset)
                            & (ApprovalRequest.enabled == True)  # noqa: E712
                        )
                    )

                    file_upload.is_processed_in_staging = True
                    if approval_request:
                        file_upload.approval_status = "PENDING"

                    self.context.log.info(
                        f"Stamped FileUpload {upload_id}: "
                        f"is_processed_in_staging=True, "
                        f"approval_status={'PENDING' if approval_request else 'null'}"
                    )
        except Exception as e:
            self.context.log.error(
                f"Failed to stamp FileUpload {upload_id} after staging: {e}"
            )

    def _get_current_approval_request(self, db, formatted_dataset: str):
        return db.scalar(
            select(ApprovalRequest).where(
                (ApprovalRequest.country == self.country_code)
                & (ApprovalRequest.dataset == formatted_dataset)
            )
        )

    def _get_uploaded_columns(self) -> list[str]:
        """Return the schema column names explicitly present in the upload file.

        Used for audit purposes only (saved on each staging row).  Silver-preservation
        is driven by the null-fallback loop in _build_upsert_records, not by this list.

        Falls back to all schema column names if no FileUpload record is found,
        which preserves backward-compatible behaviour for non-geolocation pipelines.
        """
        try:
            with get_db_context() as db:
                file_upload = db.scalar(
                    select(FileUpload).where(
                        FileUpload.id == self.config.filename_components.id
                    )
                )
                if file_upload is None:
                    raise FileNotFoundError(
                        f"FileUpload with id `{self.config.filename_components.id}` not found"
                    )
            file_upload = FileUploadConfig.from_orm(file_upload)
            columns = set(file_upload.column_to_schema_mapping.values())
            return list(columns)
        except Exception as e:
            self.context.log.warning(
                f"Could not retrieve uploaded_columns from FileUpload: {e}. "
                "Falling back to treating all schema columns as uploaded."
            )
            return [c.name for c in self.schema_columns]

    def _prepare_df(self, df: sql.DataFrame) -> sql.DataFrame:
        """Add missing columns and cast types — does NOT compute row hash."""
        df = add_missing_columns(df, self.schema_columns)
        df = transform_types(df, self.schema_name, self.context)
        # Fill nulls in NOT NULL STRING schema columns with "Unknown" so that
        # Delta NOT NULL constraints are never violated on write.
        unknown_fills = {
            col.name: f.coalesce(f.col(col.name), f.lit("Unknown"))
            for col in self.schema_columns
            if not col.nullable
            and isinstance(col.dataType, StringType)
            and col.name in df.columns
        }
        if unknown_fills:
            df = df.withColumns(unknown_fills)
        return df

    def _select_schema_cols(self, df: sql.DataFrame) -> sql.DataFrame:
        """Select only schema columns from df, dropping any extra bronze columns."""
        schema_col_names = [c.name for c in self.schema_columns]
        available = [c for c in schema_col_names if c in df.columns]
        return df.select(*available)

    def standard_transforms(self, df: sql.DataFrame) -> sql.DataFrame:
        """Backward-compatible wrapper used by other pipelines."""
        df = self._prepare_df(df)
        return compute_row_hash(df)

    def _emit_lineage(self) -> None:
        with get_db_context() as db:
            files_for_review = db.scalars(
                select(FileUpload).where(
                    (FileUpload.country == self.country_code)
                    & (FileUpload.dataset == self.config.dataset_type)
                )
            )
            upstream_filepaths = [fu.upload_path for fu in files_for_review]

        emit_lineage_base(
            upstream_datasets=upstream_filepaths,
            dataset_urn=self.config.datahub_destination_dataset_urn,
            context=self.context,
        )

    @property
    def silver_table_exists(self) -> bool:
        return check_table_exists(
            self.spark, self.schema_name, self.country_code, DataTier.SILVER
        )

    @property
    def staging_table_exists(self) -> bool:
        return check_table_exists(
            self.spark, self.schema_name, self.country_code, DataTier.STAGING
        )
