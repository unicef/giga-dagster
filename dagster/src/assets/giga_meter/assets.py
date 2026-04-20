from datetime import datetime
from typing import Optional

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.constants.constants_class import constants
from src.resources import ResourceKey
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.delta import create_schema
from src.utils.giga_meter_helpers import (
    _add_file_metadata_columns,
    _normalize_schema_for_delta,
    _read_parquet_best_effort,
    _sanitize_schema_by_category,
)
from src.utils.spark import transform_types

from dagster import Config, OpExecutionContext, Output, asset

"""
INGESTION CONTRACT

- Each run processes a single file (triggered by sensor).
- Ingestion is append-only.
- Failed reads are skipped; successful files are written to Delta.
"""


class ParquetToDeltaConfig(Config):
    upload_path: Optional[str] = constants.PING_PARQUET_PATH
    file: Optional[str] = None
    target_schema: str = "giga_meter"
    target_table: str = "connectivity_ping_checks"


# Asset
# -------------------------------------------------------------------


@asset(
    description=(
        "Reads a parquet file from ADLS, " "and appends data into a Bronze Delta table."
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

    if not config.files:
        context.log.info("No files provided in config. Skipping.")
        return Output(None)

    file_path = config.files[0]
    full_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{file_path}"

    # Read — skip on failure
    context.log.info(f"Reading: {full_path}")
    df = _read_parquet_best_effort(spark_session, full_path)

    if df is None:
        context.log.warning(f"Failed to read: {full_path}")
        return Output(None)

    # Transform
    df = _sanitize_schema_by_category(df)
    df = _normalize_schema_for_delta(df)

    metadata_values = {
        "source_file": file_path,
        "ingested_at": datetime.utcnow(),
        "ingestion_run_id": context.run_id,
    }
    df = _add_file_metadata_columns(df, metadata=metadata_values)

    df = transform_types(
        df,
        schema_name=config.target_schema,
        context=context,
        table_name=config.target_table,
    )

    # Summary
    row_count = df.count()
    context.log.info(f"Successfully processed {file_path} with {row_count} rows.")

    return Output(
        df,
        metadata={
            "processed_file": file_path,
            "row_count": row_count,
        },
    )
