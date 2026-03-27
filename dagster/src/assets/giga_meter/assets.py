from datetime import datetime
from typing import Optional

from dagster_pyspark import PySparkResource
from pyspark.sql import DataFrame, SparkSession
from src.constants.constants_class import constants
from src.resources import ResourceKey
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.delta import create_schema
from src.utils.giga_meter_helpers import (
    _add_file_metadata_columns,
    _align_to_target_schema,
    _is_file_already_processed,
    _normalize_schema_for_delta,
    _read_parquet_best_effort,
    _sanitize_schema_by_category,
)

from dagster import Config, OpExecutionContext, Output, asset

"""
INGESTION CONTRACT

- All parquet files are appended into ONE Delta table.
- Ingestion is append-only.
- Failed reads are skipped; successful files are written to Delta.
- Manifest records are written AFTER successful Delta write (in IO Manager).
- On re-run, manifest check skips already-processed files.
"""


class ParquetToDeltaConfig(Config):
    upload_path: Optional[str] = constants.PING_PARQUET_PATH
    files: Optional[list[str]] = None
    target_schema: str = "giga_meter"
    target_table: str = "connectivity_ping_checks"
    manifest_table: str = "connectivity_ping_manifest"


# Asset
# -------------------------------------------------------------------


@asset(
    description=(
        "Reads parquet files from ADLS, skips unreadable files, "
        "and appends successful reads into a Bronze Delta table."
    ),
    io_manager_key=ResourceKey.GIGA_METER_DELTA_IO_MANAGER.value,
)
def connectivity_ping_checks(
    context: OpExecutionContext,
    config: ParquetToDeltaConfig,
    spark: PySparkResource,
    adls_file_client: ADLSFileClient,
) -> Output:
    spark_session: SparkSession = spark.spark_session

    create_schema(spark_session, config.target_schema)

    base_path = config.upload_path
    if not base_path:
        raise ValueError("No upload_path provided in config.")

    # Build file list
    if config.files:
        file_paths = [
            f"{base_path}/{f}" if not f.startswith(base_path) else f
            for f in config.files
        ]
    else:
        file_paths = [base_path]

    ingestion_time = datetime.utcnow()
    accumulated_df: DataFrame | None = None
    processed_files: list[str] = []
    skipped_files: list[str] = []
    failed_files: list[str] = []

    for file_path in file_paths:
        full_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{file_path}"

        # Manifest check — skip already-processed files
        if _is_file_already_processed(
            spark_session,
            schema_name=config.target_schema,
            manifest_table=config.manifest_table,
            file_path=full_path,
        ):
            context.log.info(f"Skipping already processed: {full_path}")
            skipped_files.append(file_path)
            continue

        # Read — skip on failure
        context.log.info(f"Reading: {full_path}")
        df = _read_parquet_best_effort(spark_session, full_path)

        if df is None:
            context.log.warning(f"Failed to read: {full_path}")
            failed_files.append(file_path)
            continue

        # Transform
        df = _sanitize_schema_by_category(df)
        df = _normalize_schema_for_delta(df)

        metadata_values = {
            "_source_file": file_path,
            "_ingested_at": ingestion_time,
            "_ingestion_run_id": context.run_id,
        }
        df = _add_file_metadata_columns(df, metadata=metadata_values)

        df = _align_to_target_schema(
            df,
            spark=spark_session,
            schema_name=config.target_schema,
            table_name=config.target_table,
        )

        # Accumulate
        if accumulated_df is None:
            accumulated_df = df
        else:
            accumulated_df = accumulated_df.unionByName(df, allowMissingColumns=True)

        processed_files.append(file_path)

    # Summary
    context.log.info(
        f"Batch complete: {len(processed_files)} processed, "
        f"{len(skipped_files)} skipped, {len(failed_files)} failed."
    )

    # Write retry file for failed files (sensor reads this on next tick)
    if failed_files:
        context.log.warning(f"Failed files (will retry next run): {failed_files}")
        adls_file_client.upload_json(failed_files, constants.PING_RETRY_FILE_PATH)
    else:
        # Clear retry file when no failures
        try:
            adls_file_client.upload_json([], constants.PING_RETRY_FILE_PATH)
        except Exception:
            pass  # retry file may not exist yet

    if accumulated_df is None:
        context.log.info("No new data to write.")
        return Output(None)

    return Output(
        accumulated_df,
        metadata={
            "processed_count": len(processed_files),
            "skipped_count": len(skipped_files),
            "failed_count": len(failed_files),
            "failed_files": str(failed_files) if failed_files else "none",
        },
    )
