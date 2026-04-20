from pyspark.sql import DataFrame, SparkSession
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
    "source_file": StringType(),
    "ingested_at": TimestampType(),
    "ingestion_run_id": StringType(),
    "storage_etag": StringType(),
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
