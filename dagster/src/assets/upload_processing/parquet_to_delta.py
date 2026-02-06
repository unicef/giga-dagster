from datetime import datetime
from typing import Optional

from dagster_pyspark import PySparkResource
from pyspark.errors.exceptions.captured import AnalysisException
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

from dagster import Config, OpExecutionContext, asset
from src.constants.constants_class import constants
from src.settings import settings
from src.utils.adls import ADLSFileClient
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
# Config
# -------------------------------------------------------------------

COLUMN_CASTS: dict[str, DataType] = {
    "error_message": StringType(),
    "latency": DoubleType(),
}

FILE_METADATA_COLUMNS: dict[str, DataType] = {
    "_source_file": StringType(),
    "_ingested_at": TimestampType(),
}


class ParquetToDeltaConfig(Config):
    upload_path: Optional[str] = constants.PING_PARQUET_PATH
    target_schema: str = "giga_meter"
    target_table: str = "connectivity_ping_checks"


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
        column_name = field.name
        data_type = field.dataType

        if isinstance(data_type, TimestampType):
            df = df.withColumn(
                column_name,
                col(column_name).cast("timestamp"),
            )

        elif isinstance(data_type, DecimalType):
            df = df.withColumn(
                column_name,
                col(column_name).cast("double"),
            )

        elif isinstance(data_type, IntegerType):
            df = df.withColumn(
                column_name,
                col(column_name).cast(LongType()),
            )

    return df


def _normalize_schema_for_delta(df: DataFrame) -> DataFrame:
    for column_name, target_type in COLUMN_CASTS.items():
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                col(column_name).cast(target_type),
            )

    return df


def _add_file_metadata_columns(
    df: DataFrame,
    *,
    source_file: str,
    ingested_at: datetime,
) -> DataFrame:
    """
    Adds file-level lineage metadata.
    """
    return df.withColumn("_source_file", lit(source_file)).withColumn(
        "_ingested_at", lit(ingested_at)
    )


def _write_to_target_table(
    df: DataFrame,
    *,
    spark: SparkSession,
    schema_name: str,
    table_name: str,
) -> None:
    try:
        (
            df.write.format("delta")
            .mode("append")
            .saveAsTable(f"{schema_name}.`{table_name}`")
        )
    except AnalysisException as exc:
        if "DELTA_TABLE_NOT_FOUND" in str(exc):
            (
                df.write.format("delta")
                .mode("append")
                .saveAsTable(f"{schema_name}.`{table_name}`")
            )
        else:
            raise


# -------------------------------------------------------------------
# Asset
# -------------------------------------------------------------------


@asset(
    description=(
        "Reads parquet files from ADLS and appends them into a single "
        "Bronze Delta table. Idempotency is managed by the sensor via run keys."
    ),
)
def convert_parquets_to_delta(
    context: OpExecutionContext,
    config: ParquetToDeltaConfig,
    spark: PySparkResource,
    adls_file_client: ADLSFileClient,
) -> None:
    spark_session: SparkSession = spark.spark_session

    create_schema(spark_session, config.target_schema)

    file_path = config.upload_path

    if not file_path:
        context.log.warning("No upload path provided, skipping.")
        return

    full_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{file_path}"
    ingestion_time = datetime.utcnow()

    context.log.info(f"Processing file: {file_path}")

    df = _read_parquet_best_effort(spark_session, full_path)

    if df is None:
        context.log.error(
            f"Unreadable parquet schema or file not found, skipping file: {file_path}"
        )
        return

    context.log.info(
        f"Appending {file_path} → " f"{config.target_schema}.{config.target_table}"
    )

    df = _sanitize_schema_by_category(df)
    df = _normalize_schema_for_delta(df)
    df = _add_file_metadata_columns(
        df,
        source_file=file_path,
        ingested_at=ingestion_time,
    )

    _write_to_target_table(
        df,
        spark=spark_session,
        schema_name=config.target_schema,
        table_name=config.target_table,
    )

    context.add_output_metadata(
        {
            "processed_file": file_path,
            "status": "success",
        }
    )
