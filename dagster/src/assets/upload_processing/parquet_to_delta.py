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
from src.utils.adls import ADLSFileClient
from src.utils.delta import create_schema

MANIFEST_TABLE_NAME = "_ingestion_manifest"

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


def _read_manifest(
    spark: SparkSession,
    schema_name: str,
    table_name: str,
) -> DataFrame:
    full_table_name = f"{schema_name}.{table_name}"

    def _get_manifest() -> DataFrame:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                file_path STRING,
                file_size LONG,
                last_modified TIMESTAMP,
                checksum STRING,
                table_name STRING,
                ingested_at TIMESTAMP
            )
            USING DELTA
            """
        )

        return spark.read.table(full_table_name)

    try:
        return _get_manifest()
    except AnalysisException as exc:
        if "DELTA_TABLE_NOT_FOUND" in str(exc):
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            return _get_manifest()
        raise


def _is_new_or_modified(
    manifest_df: DataFrame,
    file_path: str,
    checksum: str,
) -> bool:
    """
    Returns True if (file_path, checksum) is not present in the manifest.
    """
    return (
        manifest_df.filter(
            (manifest_df.file_path == file_path) & (manifest_df.checksum == checksum)
        )
        .limit(1)
        .count()
        == 0
    )


def _record_manifest_entry(
    spark: SparkSession,
    schema_name: str,
    manifest_table: str,
    *,
    file_path: str,
    file_size: int,
    last_modified,
    checksum: str,
    table_name: str,
) -> None:
    row = Row(
        file_path=file_path,
        file_size=file_size,
        last_modified=last_modified,
        checksum=checksum,
        table_name=table_name,
        ingested_at=datetime.utcnow(),
    )

    (
        spark.createDataFrame([row])
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"{schema_name}.{manifest_table}")
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
            spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
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
        "Bronze Delta table using a file-level manifest and ADLS ETag–"
        "based idempotency with full lineage metadata."
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

    manifest_df = _read_manifest(
        spark_session,
        config.target_schema,
        MANIFEST_TABLE_NAME,
    )

    processed_files: list[str] = []
    skipped_files: list[str] = []

    paths = adls_file_client.list_paths(
        config.upload_path,
        recursive=True,
    )

    for path_obj in paths:
        if not path_obj.name.endswith(".parquet"):
            continue

        full_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{path_obj.name}"
        checksum = path_obj.etag
        ingestion_time = datetime.utcnow()

        if not _is_new_or_modified(
            manifest_df,
            full_path,
            checksum,
        ):
            skipped_files.append(path_obj.name)
            continue

        df = _read_parquet_best_effort(spark_session, full_path)

        if df is None:
            context.log.error(
                f"Unreadable parquet schema, skipping file: {path_obj.name}"
            )
            skipped_files.append(path_obj.name)
            continue

        context.log.info(
            f"Appending {path_obj.name} → "
            f"{config.target_schema}.{config.target_table}"
        )

        df = _sanitize_schema_by_category(df)
        df = _normalize_schema_for_delta(df)
        df = _add_file_metadata_columns(
            df,
            source_file=path_obj.name,
            ingested_at=ingestion_time,
        )

        _write_to_target_table(
            df,
            spark=spark_session,
            schema_name=config.target_schema,
            table_name=config.target_table,
        )

        _record_manifest_entry(
            spark_session,
            config.target_schema,
            MANIFEST_TABLE_NAME,
            file_path=full_path,
            file_size=path_obj.content_length,
            last_modified=path_obj.last_modified,
            checksum=checksum,
            table_name=config.target_table,
        )

        manifest_df = _read_manifest(
            spark_session,
            config.target_schema,
            MANIFEST_TABLE_NAME,
        )

        processed_files.append(path_obj.name)

    context.add_output_metadata(
        {
            "processed_files": processed_files,
            "skipped_files": skipped_files,
            "processed_count": len(processed_files),
            "skipped_count": len(skipped_files),
        }
    )
