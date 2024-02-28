import os
from io import BytesIO

import numpy as np
import pandas as pd
import src.schemas
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import NullType
from src.schemas import BaseSchema
from src.sensors.config import FileConfig
from src.spark.data_quality_tests import (
    dq_failed_rows as extract_dq_failed_rows,
    dq_passed_rows as extract_dq_passed_rows,
    row_level_checks,
)
from src.utils import adhoc as adhoc_utils
from src.utils.adls import ADLSFileClient, get_output_filepath
from src.utils.spark import transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def adhoc__load_reference_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    yield Output(raw, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="spark_csv_io_manager")
def adhoc__reference_data_quality_checks(
    context: OpExecutionContext,
    adhoc__load_reference_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    s: SparkSession = spark.spark_session
    filepath = config.filepath
    filename = filepath.split("/")[-1]
    file_stem = os.path.splitext(filename)[0]
    country_iso3 = file_stem.split("_")[0]
    schema: BaseSchema = getattr(src.schemas, config.metastore_schema)

    buffer = BytesIO(adhoc__load_reference_csv)
    buffer.seek(0)
    df: pd.DataFrame = pd.read_csv(buffer).fillna(np.nan).replace([np.nan], [None])
    df = df.loc[:, ~df.columns.duplicated(keep="first")]
    df = df.loc[:, ~df.columns.str.contains(r".+\.\d+$")]
    sdf = s.createDataFrame(df)

    columns_to_add = {}
    for column in adhoc_utils.REFERENCE_COLUMNS_TO_ADD:
        columns_to_add[column] = f.lit(None).cast(NullType())
    for column in schema.columns:
        if column.name not in sdf.columns:
            columns_to_add[column.name] = f.lit(None).cast(NullType())

    sdf = sdf.withColumns(columns_to_add)
    context.log.info(f"Renamed {len(columns_to_add)} columns")

    dq_checked = row_level_checks(sdf, "reference", country_iso3, context)
    yield Output(dq_checked, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="spark_csv_io_manager")
def adhoc__reference_dq_checks_passed(
    context: OpExecutionContext,
    adhoc__reference_data_quality_checks: sql.DataFrame,
) -> sql.DataFrame:
    dq_passed = extract_dq_passed_rows(
        adhoc__reference_data_quality_checks, "reference"
    )
    yield Output(dq_passed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="spark_csv_io_manager")
def adhoc__reference_dq_checks_failed(
    context: OpExecutionContext,
    adhoc__reference_data_quality_checks: sql.DataFrame,
) -> sql.DataFrame:
    dq_failed = extract_dq_failed_rows(
        adhoc__reference_data_quality_checks, "reference"
    )
    yield Output(dq_failed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_v2_io_manager")
def adhoc__publish_reference_to_gold(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_dq_checks_passed: sql.DataFrame,
) -> sql.DataFrame:
    gold = transform_types(
        adhoc__reference_dq_checks_passed, config.metastore_schema, context
    )
    yield Output(gold, metadata={"filepath": get_output_filepath(context)})
