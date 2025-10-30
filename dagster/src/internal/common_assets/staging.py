import enum

from delta import DeltaTable
from models.approval_requests import ApprovalRequest
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from sqlalchemy import select, text, update

from dagster import OpExecutionContext
from src.constants import DataTier
from src.internal.merge import partial_in_cluster_merge
from src.spark.transform_functions import add_missing_columns
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_lineage import emit_lineage_base
from src.utils.db.primary import get_db_context
from src.utils.db.trino import get_db_context as get_trino_context
from src.utils.delta import (
    build_deduped_delete_query,
    build_deduped_merge_query,
    check_table_exists,
    create_delta_table,
    create_schema,
    execute_query_with_error_handler,
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


class StagingChangeTypeEnum(enum.Enum):
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class StagingStep:
    def __init__(
        self,
        context: OpExecutionContext,
        config: FileConfig,
        adls_file_client: ADLSFileClient,
        spark: SparkSession,
        change_type: StagingChangeTypeEnum,
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
        staging = self._process_staging_changes(upstream_df)
        if staging is None:
            return None

        self._update_approval_request_status(staging)
        self._emit_lineage()

        return staging

    def _process_staging_changes(
        self, upstream_df: sql.DataFrame | list[str]
    ) -> sql.DataFrame | None:
        """Process staging changes based on silver table existence."""
        if self.silver_table_exists:
            return self._process_with_silver_table(upstream_df)
        else:
            return self._process_without_silver_table(upstream_df)

    def _process_with_silver_table(
        self, upstream_df: sql.DataFrame | list[str]
    ) -> sql.DataFrame:
        """Process staging changes when silver table exists."""
        if not self.staging_table_exists:
            # If silver table exists and no staging table exists, clone it to staging
            self.create_staging_table_from_silver()

        self.sync_schema_staging()

        # If silver table exists and staging table exists, merge files for review to existing staging table
        if self.change_type != StagingChangeTypeEnum.DELETE:
            staging = self.standard_transforms(upstream_df)
            staging = self.upsert_rows(staging)
        else:
            staging = self.delete_rows(upstream_df)

        return staging

    def _process_without_silver_table(
        self, upstream_df: sql.DataFrame | list[str]
    ) -> sql.DataFrame | None:
        """Process staging changes when silver table does not exist."""
        if self.change_type != StagingChangeTypeEnum.UPDATE:
            return (
                None  # Cannot delete rows when silver and staging tables do not exist
            )

        staging = self.standard_transforms(upstream_df)

        if self.staging_table_exists:
            self.sync_schema_staging()
            staging = self.upsert_rows(staging)
        else:
            self.create_empty_staging_table()
            (
                staging.write.option("mergeSchema", "true")
                .format("delta")
                .mode("append")
                .saveAsTable(self.staging_table_name)
            )

        self.context.log.info(f"Full {staging.count()=}")
        return staging

    def _get_pre_update_row_count(self) -> int | None:
        """Get row count from staging table before update."""
        if not self.staging_table_exists:
            return None

        try:
            with get_trino_context() as trino_db:
                # Table name is constructed from trusted config sources, not user input
                result = trino_db.execute(
                    text(f"SELECT COUNT(*) as count FROM {self.staging_table_name}")  # nosec B608
                )
                row_count = result.scalar()
                self.context.log.info(f"Pre-update staging row count: {row_count}")
                return row_count
        except Exception as e:
            self.context.log.warning(
                f"Failed to query staging row count: {e}. Proceeding with DB update."
            )
            return None

    def _update_approval_request_status(self, staging: sql.DataFrame) -> None:
        """Update ApprovalRequest status if conditions are met."""
        pre_update_row_count = self._get_pre_update_row_count()
        formatted_dataset = f"School {self.config.dataset_type.capitalize()}"

        with get_db_context() as db:
            try:
                current_request = self._get_current_approval_request(
                    db, formatted_dataset
                )
                if current_request is None:
                    self.context.log.warning(
                        f"No ApprovalRequest found for {self.country_code} - {formatted_dataset}"
                    )
                    return

                if not self._should_update_enabled(
                    current_request, pre_update_row_count, formatted_dataset
                ):
                    return

                self._execute_enabled_update(db, formatted_dataset)
            except Exception as e:
                self._handle_update_error(db, e, formatted_dataset)

    def _get_current_approval_request(self, db, formatted_dataset: str):
        """Get current ApprovalRequest from database."""
        return db.scalar(
            select(ApprovalRequest).where(
                (ApprovalRequest.country == self.country_code)
                & (ApprovalRequest.dataset == formatted_dataset)
            )
        )

    def _should_update_enabled(
        self, current_request, pre_update_row_count: int | None, formatted_dataset: str
    ) -> bool:
        """Check if enabled flag should be updated."""
        if current_request.enabled:
            self.context.log.info(
                f"ApprovalRequest already enabled for {self.country_code} - {formatted_dataset}. Skipping update."
            )
            return False

        if (
            pre_update_row_count is not None
            and pre_update_row_count == 0
            and self.change_type == StagingChangeTypeEnum.UPDATE
        ):
            self.context.log.info(
                f"No changes detected (row count: {pre_update_row_count}). Skipping enabled=True update."
            )
            return False

        return True

    def _execute_enabled_update(self, db, formatted_dataset: str) -> None:
        """Execute update to set enabled=True."""
        result = db.execute(
            update(ApprovalRequest)
            .where(
                (ApprovalRequest.country == self.country_code)
                & (ApprovalRequest.dataset == formatted_dataset)
                & (~ApprovalRequest.enabled)  # Only update if False
            )
            .values(
                {
                    ApprovalRequest.enabled: True,
                    ApprovalRequest.is_merge_processing: False,
                }
            ),
        )
        if result.rowcount > 0:
            db.commit()
            self.context.log.info(
                f"Successfully set enabled=True for {self.country_code} - {formatted_dataset}"
            )
        else:
            self.context.log.info(
                "No rows updated (already enabled or state changed). Skipping commit."
            )

    def _handle_update_error(
        self, db, error: Exception, formatted_dataset: str
    ) -> None:
        """Handle errors during ApprovalRequest update."""
        self.context.log.error(
            f"Failed to update ApprovalRequest for {self.country_code} - {formatted_dataset}: {error}"
        )
        db.rollback()
        # Try to rollback Delta changes if possible
        if self.staging_table_exists:
            try:
                # Log warning but don't fail - Delta operations are atomic
                self.context.log.warning(
                    "Delta table changes may have been committed. Manual review may be needed."
                )
            except Exception as delta_err:
                self.context.log.warning(
                    f"Could not access Delta table for rollback: {delta_err}"
                )
        raise

    def _emit_lineage(self) -> None:
        """Emit lineage information."""
        # Get files for review in a separate DB context
        with get_db_context() as db:
            files_for_review = db.scalars(
                select(FileUpload).where(
                    (FileUpload.country == self.country_code)
                    & (FileUpload.dataset == self.config.dataset_type)
                )
            )
            upstream_filepaths = [f.upload_path for f in files_for_review]

        emit_lineage_base(
            upstream_datasets=upstream_filepaths,
            dataset_urn=self.config.datahub_destination_dataset_urn,
            context=self.context,
        )

    @property
    def silver_table_exists(self) -> bool:
        # Metastore entry must be present AND ADLS path must be a valid Delta Table
        return check_table_exists(
            self.spark, self.schema_name, self.country_code, DataTier.SILVER
        )

    @property
    def staging_table_exists(self) -> bool:
        # Metastore entry must be present AND ADLS path must be a valid Delta Table
        return check_table_exists(
            self.spark, self.schema_name, self.country_code, DataTier.STAGING
        )

    def create_staging_table_from_silver(self):
        self.context.log.info("Creating staging from silver if not exists...")
        silver = (
            DeltaTable.forName(self.spark, self.silver_table_name)
            .alias("silver")
            .toDF()
        )
        create_schema(self.spark, self.staging_tier_schema_name)
        create_delta_table(
            self.spark,
            self.staging_tier_schema_name,
            self.country_code,
            self.schema_columns,
            self.context,
            if_not_exists=True,
        )
        silver.write.format("delta").mode("append").saveAsTable(self.staging_table_name)

    def create_empty_staging_table(self):
        self.context.log.info("Creating empty staging table...")
        create_schema(self.spark, self.staging_tier_schema_name)
        create_delta_table(
            self.spark,
            self.staging_tier_schema_name,
            self.country_code,
            self.schema_columns,
            self.context,
            if_not_exists=True,
        )

    def sync_schema_staging(self):
        """Update the schema of existing delta tables based on the reference schema delta tables."""
        self.context.log.info("Checking for schema update...")
        updated_schema = StructType(self.schema_columns)
        updated_columns = sorted(updated_schema.fieldNames())

        existing_df = DeltaTable.forName(self.spark, self.staging_table_name).toDF()
        existing_columns = sorted(existing_df.schema.fieldNames())

        # Sync changes in nullability flags
        alter_sql = f"ALTER TABLE {self.staging_table_name}"
        alter_stmts = []
        for column in existing_df.schema:
            if (
                match_ := next(
                    (c for c in updated_schema if c.name == column.name), None
                )
            ) is not None:
                if match_.nullable != column.nullable:
                    if match_.nullable:
                        alter_stmts.append(f"ALTER COLUMN {column.name} DROP NOT NULL")
                    else:
                        alter_stmts.append(f"ALTER COLUMN {column.name} SET NOT NULL")

        has_nullability_changed = len(alter_stmts) > 0
        has_schema_changed = updated_columns != existing_columns

        # Sync changes in columns & data types
        if has_schema_changed:
            self.context.log.info("Updating schema...")
            updated_schema_df = self.spark.createDataFrame([], schema=updated_schema)
            (
                updated_schema_df.write.option("mergeSchema", "true")
                .format("delta")
                .mode("append")
                .saveAsTable(self.staging_table_name)
            )

        if has_nullability_changed:
            alter_sql = [f"{alter_sql} {alter_stmt}" for alter_stmt in alter_stmts]
            for stmnt in alter_sql:
                self.spark.sql(stmnt).show()

        if has_schema_changed or has_nullability_changed:
            self.reload_schema()

    def reload_schema(self):
        self.schema_columns = get_schema_columns(self.spark, self.schema_name)

    def standard_transforms(self, df: sql.DataFrame):
        self.context.log.info("Performing standard transforms...")
        df = add_missing_columns(df, self.schema_columns)
        df = transform_types(df, self.schema_name, self.context)
        return compute_row_hash(df)

    def upsert_rows(self, df: sql.DataFrame):
        self.context.log.info("Performing upsert...")
        staging_dt = DeltaTable.forName(self.spark, self.staging_table_name)
        update_columns = [
            c.name for c in self.schema_columns if c.name != self.primary_key
        ]
        df = partial_in_cluster_merge(
            staging_dt.toDF(),
            df,
            self.primary_key,
            column_names=[c.name for c in self.schema_columns],
        )
        query = build_deduped_merge_query(
            staging_dt,
            df,
            self.primary_key,
            update_columns,
        )

        if query is not None:
            execute_query_with_error_handler(
                self.spark,
                query,
                self.staging_tier_schema_name,
                self.country_code,
                self.context,
            )
        return staging_dt.toDF()

    def delete_rows(self, df: list[str]):
        self.context.log.info("Performing delete...")
        staging_dt = DeltaTable.forName(self.spark, self.staging_table_name)

        query = build_deduped_delete_query(staging_dt, df, self.primary_key)

        if query is not None:
            execute_query_with_error_handler(
                self.spark,
                query,
                self.staging_tier_schema_name,
                self.country_code,
                self.context,
            )
        return staging_dt.toDF()
