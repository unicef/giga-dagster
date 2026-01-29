from datetime import datetime
from typing import Optional

from dagster_pyspark import PySparkResource
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, concat_ws, sha2

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
- Idempotency is enforced via (file_path, checksum).
- Same file + same checksum → skipped.
- Same file + different checksum → appended again.
- Manifest is the source of truth.
"""


# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------


class ParquetToDeltaConfig(Config):
    upload_path: Optional[str] = constants.PING_PARQUET_PATH
    target_schema: str = "giga_meter"
    target_table: str = "connectivity_ping_checks"


# -------------------------------------------------------------------
# Checksum
# -------------------------------------------------------------------


def _compute_checksum(
    spark: SparkSession,
    parquet_path: str,
) -> str:
    df = spark.read.parquet(parquet_path)

    return (
        df.select(
            sha2(
                concat_ws(
                    "||",
                    *[col(column).cast("string") for column in df.columns],
                ),
                256,
            ).alias("checksum")
        )
        .limit(1)
        .collect()[0]["checksum"]
    )


# -------------------------------------------------------------------
# Manifest helpers
# -------------------------------------------------------------------


def _read_manifest(
    spark: SparkSession,
    schema_name: str,
    table_name: str,
) -> DataFrame:
    full_table_name = f"{schema_name}.{table_name}"

    def _get_manifest():
        # MUST exist before creating tables
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


@asset(
    description=(
        "Reads parquet files from ADLS and appends them into a single "
        "Bronze Delta table using a manifest-based ingestion strategy."
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

    paths = adls_file_client.list_paths(config.upload_path, recursive=True)

    for path_obj in paths:
        if not path_obj.name.endswith(".parquet"):
            continue

        full_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{path_obj.name}"
        checksum = _compute_checksum(spark_session, full_path)

        if not _is_new_or_modified(manifest_df, full_path, checksum):
            skipped_files.append(path_obj.name)
            continue

        context.log.info(
            f"Appending {path_obj.name} → "
            f"{config.target_schema}.{config.target_table}"
        )

        df = spark_session.read.parquet(full_path)

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
