from datetime import datetime
from typing import Optional

from dagster_pyspark import PySparkResource
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    DataType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from dagster import Config, OpExecutionContext, asset
from src.constants.constants_class import constants
from src.settings import settings
from src.utils.delta import create_schema

"""
INGESTION CONTRACT

- All parquet files are appended into ONE Delta table.
- Ingestion is append-only.
- Idempotency is enforced via (file_path, storage_checksum).
- storage_checksum = ADLS ETag.
- Same file + same ETag → skipped.
- Same file + different ETag → appended again.
- Manifest is file-level and is the source of truth.
"""


# -------------------------------------------------------------------
# Canonical Type Enforcement
# -------------------------------------------------------------------

COLUMN_CASTS: dict[str, DataType] = {
    "error_message": StringType(),
    "latency": DoubleType(),
    "deleted_at": LongType(),
}

FILE_METADATA_COLUMNS: dict[str, DataType] = {
    "_source_file": StringType(),
    "_ingested_at": TimestampType(),
    "_ingestion_run_id": StringType(),
    "_storage_etag": StringType(),
}


class ParquetToDeltaConfig(Config):
    upload_path: Optional[str] = constants.PING_PARQUET_PATH
    etag: str
    target_schema: str = "giga_meter"
    target_table: str = "connectivity_ping_checks"
    manifest_table: str = "connectivity_ping_manifest"


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _read_parquet_best_effort(
    spark: SparkSession,
    full_path: str,
) -> DataFrame | None:
    try:
        return spark.read.parquet(full_path)
    except Exception:
        return None


def _sanitize_schema_by_category(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType

        if isinstance(dtype, TimestampType):
            df = df.withColumn(name, col(name).cast("timestamp"))

        elif isinstance(dtype, DecimalType):
            df = df.withColumn(name, col(name).cast("double"))

        elif isinstance(dtype, IntegerType):
            df = df.withColumn(name, col(name).cast(LongType()))

    return df


def _normalize_schema_for_delta(df: DataFrame) -> DataFrame:
    for column_name, target_type in COLUMN_CASTS.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(target_type))
    return df


def _add_file_metadata_columns(
    df: DataFrame,
    *,
    metadata: dict[str, object],
) -> DataFrame:
    for column_name, data_type in FILE_METADATA_COLUMNS.items():
        if column_name in metadata:
            df = df.withColumn(
                column_name,
                lit(metadata[column_name]).cast(data_type),
            )
    return df


def _align_to_target_schema(
    df: DataFrame,
    *,
    spark: SparkSession,
    schema_name: str,
    table_name: str,
) -> DataFrame:
    """
    Force dataframe schema to match existing Delta table schema.
    Prevents IntegerType vs LongType conflicts.
    """
    try:
        target_df = spark.table(f"{schema_name}.{table_name}")
    except AnalysisException:
        return df  # table does not exist yet

    target_schema = target_df.schema

    for field in target_schema.fields:
        if field.name in df.columns:
            df = df.withColumn(
                field.name,
                col(field.name).cast(field.dataType),
            )

    return df


def _write_to_target_table(
    df: DataFrame,
    *,
    spark: SparkSession,
    schema_name: str,
    table_name: str,
) -> None:
    table_full_name = f"{schema_name}.`{table_name}`"

    table_exists = spark.catalog.tableExists(schema_name, table_name)

    if not table_exists:
        (df.write.format("delta").mode("overwrite").saveAsTable(table_full_name))
        return

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_full_name)
    )


def _is_file_already_processed(
    spark: SparkSession,
    *,
    schema_name: str,
    manifest_table: str,
    file_path: str,
    etag: str,
) -> bool:
    try:
        manifest_df = spark.table(f"{schema_name}.{manifest_table}")
    except AnalysisException:
        return False

    return (
        manifest_df.filter((col("file_path") == file_path) & (col("etag") == etag))
        .limit(1)
        .count()
        > 0
    )


def _append_manifest_record(
    spark: SparkSession,
    *,
    schema_name: str,
    manifest_table: str,
    file_path: str,
    etag: str,
    ingested_at: datetime,
) -> None:
    manifest_df = spark.createDataFrame(
        [
            Row(
                file_path=file_path,
                etag=etag,
                ingested_at=ingested_at,
            )
        ]
    )

    table_exists = spark.catalog.tableExists(schema_name, manifest_table)

    if not table_exists:
        (
            manifest_df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(f"{schema_name}.{manifest_table}")
        )
    else:
        (
            manifest_df.write.format("delta")
            .mode("append")
            .saveAsTable(f"{schema_name}.{manifest_table}")
        )


# -------------------------------------------------------------------
# Asset
# -------------------------------------------------------------------


@asset(
    description=(
        "Reads parquet files from ADLS and appends them into a single "
        "Bronze Delta table. Idempotency enforced via manifest table."
    ),
)
def convert_parquets_to_delta(
    context: OpExecutionContext,
    config: ParquetToDeltaConfig,
    spark: PySparkResource,
) -> None:
    spark_session: SparkSession = spark.spark_session

    create_schema(spark_session, config.target_schema)

    file_path = config.upload_path
    etag = config.etag

    if not file_path:
        context.log.warning("No upload path provided, skipping.")
        return

    full_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{file_path}"
    ingestion_time = datetime.utcnow()

    # -------------------------------
    # Idempotency Check
    # -------------------------------

    if _is_file_already_processed(
        spark_session,
        schema_name=config.target_schema,
        manifest_table=config.manifest_table,
        file_path=file_path,
        etag=etag,
    ):
        context.log.info(f"Skipping already processed file: {file_path}")
        return

    # -------------------------------
    # Read File
    # -------------------------------

    context.log.info(f"Processing file: {file_path}")

    df = _read_parquet_best_effort(spark_session, full_path)

    if df is None:
        context.log.error(f"Unreadable parquet file: {file_path}")
        return

    # -------------------------------
    # Transform
    # -------------------------------

    df = _sanitize_schema_by_category(df)
    df = _normalize_schema_for_delta(df)

    metadata_values = {
        "_source_file": file_path,
        "_ingested_at": ingestion_time,
        "_ingestion_run_id": context.run_id,
        "_storage_etag": etag,
    }

    df = _add_file_metadata_columns(
        df,
        metadata=metadata_values,
    )

    # Align with existing table schema (CRITICAL FIX)
    df = _align_to_target_schema(
        df,
        spark=spark_session,
        schema_name=config.target_schema,
        table_name=config.target_table,
    )

    # -------------------------------
    # Write Target
    # -------------------------------

    _write_to_target_table(
        df,
        spark=spark_session,
        schema_name=config.target_schema,
        table_name=config.target_table,
    )

    # -------------------------------
    # Update Manifest
    # -------------------------------

    _append_manifest_record(
        spark_session,
        schema_name=config.target_schema,
        manifest_table=config.manifest_table,
        file_path=file_path,
        etag=etag,
        ingested_at=ingestion_time,
    )

    context.add_output_metadata(
        {
            "processed_file": file_path,
            "etag": etag,
            "status": "success",
        }
    )
