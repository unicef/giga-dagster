import os
from io import BytesIO

import numpy as np
import pandas as pd
import sentry_sdk
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import NullType
from src.data_quality_checks.utils import (
    dq_split_failed_rows as extract_dq_failed_rows,
    dq_split_passed_rows as extract_dq_passed_rows,
    row_level_checks,
)
from src.resources import ResourceKey
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub
from src.utils.metadata import get_output_metadata
from src.utils.op_config import FileConfig
from src.utils.schema import get_schema_columns, get_schema_columns_datahub
from src.utils.sentry import log_op_context
from src.utils.spark import compute_row_hash, transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def adhoc__load_reference_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    emit_metadata_to_datahub(
        context,
        country_code=config.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    yield Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_data_quality_checks(
    context: OpExecutionContext,
    adhoc__load_reference_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> pd.DataFrame:
    s: SparkSession = spark.spark_session
    filepath = config.filepath
    filename = filepath.split("/")[-1]
    file_stem = os.path.splitext(filename)[0]
    country_iso3 = file_stem.split("_")[0]

    with BytesIO(adhoc__load_reference_csv) as buffer:
        buffer.seek(0)
        df: pd.DataFrame = pd.read_csv(buffer).fillna(np.nan).replace([np.nan], [None])

    df = df.loc[:, ~df.columns.duplicated(keep="first")]
    df = df.loc[:, ~df.columns.str.contains(r".+\.\d+$")]
    sdf = s.createDataFrame(df)

    columns = get_schema_columns(s, config.metastore_schema)
    columns_to_add = {}
    for column in columns:
        if column.name not in sdf.columns:
            columns_to_add[column.name] = f.lit(None).cast(NullType())

    sdf = sdf.withColumns(columns_to_add)
    context.log.info(f"Renamed {len(columns_to_add)} columns")

    dq_checked = row_level_checks(sdf, "reference", country_iso3, context)
    dq_checked = transform_types(dq_checked, config.metastore_schema, context)
    yield Output(
        dq_checked.toPandas(),
        metadata=get_output_metadata(config),
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_dq_checks_passed(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_data_quality_checks: sql.DataFrame,
) -> pd.DataFrame:
    dq_passed = extract_dq_passed_rows(
        adhoc__reference_data_quality_checks,
        "reference",
    )
    dq_passed = dq_passed.withColumn(
        "signature",
        f.sha2(f.concat_ws("|", *sorted(dq_passed.columns)), 256),
    )
    context.log.info(f"Calculated SHA256 signature for {dq_passed.count()} rows")
    yield Output(
        dq_passed.toPandas(),
        metadata=get_output_metadata(config),
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_dq_checks_failed(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_data_quality_checks: sql.DataFrame,
) -> pd.DataFrame:
    dq_failed = extract_dq_failed_rows(
        adhoc__reference_data_quality_checks,
        "reference",
    )
    yield Output(
        dq_failed.toPandas(),
        metadata=get_output_metadata(config),
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__publish_reference_to_gold(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_dq_checks_passed: sql.DataFrame,
    spark: PySparkResource,
) -> sql.DataFrame:
    gold = transform_types(
        adhoc__reference_dq_checks_passed,
        config.metastore_schema,
        context,
    )
    gold = compute_row_hash(gold)

    schema_reference = get_schema_columns_datahub(
        spark.spark_session,
        config.metastore_schema,
    )
    try:
        emit_metadata_to_datahub(
            context,
            schema_reference=schema_reference,
            country_code=config.country_code,
            dataset_urn=config.datahub_destination_dataset_urn,
        )
    except Exception as error:
        context.log.error(f"Error on Datahub Emit Metadata: {error}")
        log_op_context(context)
        sentry_sdk.capture_exception(error=error)

    yield Output(gold, metadata=get_output_metadata(config))
