import os
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
INGESTION CONTRACT – READ THIS BEFORE CHANGING ANYTHING

This asset implements BRONZE ingestion with the following guarantees:

1. One Parquet file maps to exactly one Delta table.
   - Table name is derived from the Parquet filename (without extension).

2. Ingestion is APPEND-ONLY by design.
   - Existing Delta tables are never overwritten.
   - Reprocessing appends data if (and only if) file content changes.

3. Idempotency is enforced via a Delta manifest table:
   - bronze._ingestion_manifest
   - A file is considered ingested if (file_path, checksum) exists.

4. A Parquet file is reprocessed ONLY if its checksum changes.
   - Filename reuse without content change is ignored.
   - Filename reuse with content change is ingested again.

5. Failure semantics:
   - If Delta write succeeds but manifest write fails,
     manual reconciliation is required.
   - The manifest is the source of truth for ingestion state.

DO NOT:
- Change write mode to overwrite without redesigning manifest logic.
- Ingest multiple files into the same table without redesigning state tracking.
- Enable schema auto-merge without understanding downstream impact.
"""


class ParquetToDeltaConfig(Config):
    upload_path: Optional[str] = constants.PING_PARQUET_PATH
    target_schema: str = "bronze"


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
                    *[col(c).cast("string") for c in df.columns],
                ),
                256,
            ).alias("checksum")
        )
        .limit(1)
        .collect()[0]["checksum"]
    )


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
            # This handles the case where the Metastore has a stale entry
            # but the actual Delta files are missing.
            print(f"Dropping stale table {full_table_name} and retrying...")
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


@asset(
    description=(
        "Reads parquet files from ADLS upload folder and converts them "
        "to Bronze Delta tables using a manifest-based ingestion strategy."
    ),
)
def convert_parquets_to_delta(
    context: OpExecutionContext,
    config: ParquetToDeltaConfig,
    spark: PySparkResource,
    adls_file_client: ADLSFileClient,
) -> None:
    spark_session: SparkSession = spark.spark_session
    upload_path = config.upload_path
    schema_name = config.target_schema

    # Ensure schema exists BEFORE touching manifest
    create_schema(spark_session, schema_name)

    manifest_df = _read_manifest(
        spark_session,
        schema_name,
        MANIFEST_TABLE_NAME,
    )

    processed_tables: list[str] = []
    skipped_tables: list[str] = []

    paths = adls_file_client.list_paths(upload_path, recursive=True)

    for path_obj in paths:
        path_name = path_obj.name

        if not path_name.endswith(".parquet"):
            continue

        full_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{path_name}"

        file_size = path_obj.content_length
        last_modified = path_obj.last_modified

        table_name = os.path.splitext(os.path.basename(path_name))[0].replace("-", "_")

        checksum = _compute_checksum(spark_session, full_path)

        if not _is_new_or_modified(
            manifest_df,
            full_path,
            checksum,
        ):
            skipped_tables.append(table_name)
            continue

        context.log.info(f"Processing {path_name} -> {schema_name}.{table_name}")

        try:
            df = spark_session.read.parquet(full_path)

            try:
                (
                    df.write.format("delta")
                    .mode("append")
                    .saveAsTable(f"{schema_name}.`{table_name}`")
                )
            except AnalysisException as exc:
                if "DELTA_TABLE_NOT_FOUND" in str(exc):
                    context.log.warning(
                        f"Table {schema_name}.{table_name} not found in storage "
                        "but exists in metastore. Dropping and retrying."
                    )
                    spark_session.sql(
                        f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
                    )
                    (
                        df.write.format("delta")
                        .mode("append")
                        .saveAsTable(f"{schema_name}.`{table_name}`")
                    )
                else:
                    raise

            _record_manifest_entry(
                spark_session,
                schema_name,
                MANIFEST_TABLE_NAME,
                file_path=full_path,
                file_size=file_size,
                last_modified=last_modified,
                checksum=checksum,
                table_name=table_name,
            )

            # Refresh manifest to avoid stale reads in the same run
            manifest_df = _read_manifest(
                spark_session,
                schema_name,
                MANIFEST_TABLE_NAME,
            )

            processed_tables.append(table_name)

        except Exception:
            context.log.exception(f"Failed processing parquet file: {path_name}")
            continue

    context.add_output_metadata(
        {
            "processed_tables": processed_tables,
            "skipped_tables": skipped_tables,
            "processed_count": len(processed_tables),
            "skipped_count": len(skipped_tables),
        }
    )
