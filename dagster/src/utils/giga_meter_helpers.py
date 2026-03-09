from datetime import datetime

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


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _read_parquet_best_effort(
    spark: SparkSession,
    full_path: str,
) -> DataFrame | None:
    """
    Attempts to read a Parquet file from the given path.
    Returns the DataFrame if successful, or None if the file is unreadable.
    """
    try:
        return spark.read.parquet(full_path)
    except Exception:
        return None


def _sanitize_schema_by_category(df: DataFrame) -> DataFrame:
    """
    Sanitizes standard data types in a DataFrame globally across all columns.
    Converts TimestampType to timestamp, DecimalType to double, and IntegerType to LongType
    to ensure smooth Delta table merging and insertion.
    """
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
    """
    Casts specific columns required by the ingestion contract into their
    canonical target data types as defined in COLUMN_CASTS.
    """
    for column_name, target_type in COLUMN_CASTS.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(target_type))
    return df


def _add_file_metadata_columns(
    df: DataFrame,
    *,
    metadata: dict[str, object],
) -> DataFrame:
    """
    Appends mandatory tracking metadata columns (like source file, ingestion time,
    run ID, and storage ETag) to the DataFrame for provenance and idempotency checks.
    """
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


def _is_file_already_processed(
    spark: SparkSession,
    *,
    schema_name: str,
    manifest_table: str,
    file_path: str,
    etag: str | None = None,
) -> bool:
    """
    Checks the manifest table to determine whether a given file
    has already been successfully ingested into the target table.
    When etag is provided, checks both file_path and etag.
    Otherwise checks file_path only.
    """
    try:
        manifest_df = spark.table(f"{schema_name}.{manifest_table}")
    except AnalysisException:
        return False

    condition = col("file_path") == file_path
    if etag is not None:
        condition = condition & (col("etag") == etag)

    return manifest_df.filter(condition).limit(1).count() > 0


def _append_manifest_record(
    spark: SparkSession,
    *,
    schema_name: str,
    manifest_table: str,
    file_path: str,
    etag: str,
    ingested_at: datetime,
) -> None:
    """
    Records a successful file ingestion into the manifest Delta table.
    Used for recording file provenance and future idempotency checks.
    """
    manifest_df = spark.createDataFrame(
        [
            Row(
                file_path=file_path,
                etag=etag,
                ingested_at=ingested_at,
            )
        ]
    )

    (
        manifest_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{schema_name}.{manifest_table}")
    )
