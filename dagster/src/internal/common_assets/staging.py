import enum

from delta import DeltaTable
from icecream import ic
from models.approval_requests import ApprovalRequest
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from sqlalchemy import select, update

from dagster import OpExecutionContext
from src.constants import DataTier
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_lineage import emit_lineage_base
from src.utils.db.primary import get_db_context
from src.utils.delta import (
    build_deduped_delete_query,
    build_deduped_merge_query,
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
        self.silver_table_path = f"{settings.SPARK_WAREHOUSE_DIR}/{self.silver_tier_schema_name}.db/{self.country_code.lower()}"
        self.staging_table_path = f"{settings.SPARK_WAREHOUSE_DIR}/{self.staging_tier_schema_name}.db/{self.country_code.lower()}"

    def __call__(self, upstream_df: sql.DataFrame | list[str]) -> sql.DataFrame | None:
        if self.silver_table_exists:
            if not self.staging_table_exists:
                # If silver table exists and no staging table exists, clone it to staging
                self.create_staging_table_from_silver()

            self.sync_schema()

            # If silver table exists and staging table exists, merge files for review to existing staging table
            if self.change_type != StagingChangeTypeEnum.DELETE:
                staging = self.standard_transforms(upstream_df)
                staging = self.upsert_rows(staging)
            else:
                staging = self.delete_rows(upstream_df)

        else:
            # If silver table does not exist, merge files for review into one spark dataframe
            if self.change_type == StagingChangeTypeEnum.UPDATE:
                staging = self.standard_transforms(upstream_df)

                if self.staging_table_exists:
                    self.sync_schema()
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
            else:
                return None  # Cannot delete rows when silver and staging tables do not exist

        formatted_dataset = f"School {self.config.dataset_type.capitalize()}"
        with get_db_context() as db:
            db.execute(
                update(ApprovalRequest)
                .where(
                    (ApprovalRequest.country == self.country_code)
                    & (ApprovalRequest.dataset == formatted_dataset)
                )
                .values(
                    {
                        ApprovalRequest.enabled: True,
                        ApprovalRequest.is_merge_processing: False,
                    }
                ),
            )
            db.commit()

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

        return staging

    @property
    def silver_table_exists(self) -> bool:
        # Metastore entry must be present AND ADLS path must be a valid Delta Table
        return ic(
            self.spark.catalog.tableExists(self.silver_table_name)
            and DeltaTable.isDeltaTable(self.spark, self.silver_table_path)
        )

    @property
    def staging_table_exists(self) -> bool:
        # Metastore entry must be present AND ADLS path must be a valid Delta Table
        return ic(
            self.spark.catalog.tableExists(self.staging_table_name)
            and DeltaTable.isDeltaTable(self.spark, self.staging_table_path)
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

    def sync_schema(self):
        """Update the schema of existing delta tables based on the reference schema delta tables."""
        self.context.log.info("Checking for schema update...")
        updated_schema = StructType(self.schema_columns)
        updated_columns = sorted(updated_schema.fieldNames())

        existing_df = DeltaTable.forName(self.spark, self.staging_table_name).toDF()
        existing_columns = sorted(existing_df.schema.fieldNames())

        if updated_columns != existing_columns:
            self.context.log.info("Updating schema...")
            empty_data = self.spark.sparkContext.emptyRDD()
            updated_schema_df = self.spark.createDataFrame(
                data=empty_data, schema=updated_schema
            )

            (
                updated_schema_df.write.option("mergeSchema", "true")
                .format("delta")
                .mode("append")
                .saveAsTable(self.staging_table_name)
            )
            self.reload_schema()

    def reload_schema(self):
        self.schema_columns = get_schema_columns(self.spark, self.schema_name)

    def standard_transforms(self, df: sql.DataFrame):
        self.context.log.info("Performing standard transforms...")
        df = transform_types(df, self.schema_name, self.context)
        return compute_row_hash(df)

    def upsert_rows(self, df: sql.DataFrame):
        self.context.log.info("Performing upsert...")
        staging_dt = DeltaTable.forName(self.spark, self.staging_table_name)
        update_columns = [
            c.name for c in self.schema_columns if c.name != self.primary_key
        ]
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
