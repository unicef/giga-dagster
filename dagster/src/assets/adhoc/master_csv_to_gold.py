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
from src.utils.adls import ADLSFileClient, get_output_filepath
from src.utils.logger import ContextLoggerWithLoguruFallback
from src.utils.spark import transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def adhoc__load_master_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    yield Output(raw, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_data_quality_checks(
    context: OpExecutionContext,
    adhoc__load_master_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> pd.DataFrame:
    logger = ContextLoggerWithLoguruFallback(context)

    s: SparkSession = spark.spark_session
    filepath = config.filepath
    filename = filepath.split("/")[-1]
    file_stem = os.path.splitext(filename)[0]
    country_iso3 = file_stem.split("_")[0]

    schema_name = config.metastore_schema
    schema: BaseSchema = getattr(src.schemas, schema_name)

    buffer = BytesIO(adhoc__load_master_csv)
    buffer.seek(0)
    df = pd.read_csv(buffer).fillna(np.nan).replace([np.nan], [None])
    sdf = s.createDataFrame(df)

    columns_to_add = {
        col.name: f.lit(None).cast(NullType())
        for col in schema.columns
        if col.name not in sdf.columns
    }

    sdf = logger.passthrough(
        sdf.withColumns(columns_to_add), f"Added {len(columns_to_add)} missing columns"
    )

    dq_checked = logger.passthrough(
        row_level_checks(sdf, "master", country_iso3, context),
        "Row level checks completed",
    )
    dq_checked = transform_types(dq_checked, config.metastore_schema, context)
    yield Output(
        dq_checked.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_dq_checks_passed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
) -> pd.DataFrame:
    dq_passed = extract_dq_passed_rows(adhoc__master_data_quality_checks, "master")
    yield Output(
        dq_passed.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_dq_checks_failed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
) -> sql.DataFrame:
    dq_failed = extract_dq_failed_rows(adhoc__master_data_quality_checks, "master")
    yield Output(
        dq_failed.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_delta_v2_io_manager")
def adhoc__publish_master_to_gold(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__master_dq_checks_passed: sql.DataFrame,
) -> sql.DataFrame:
    gold = transform_types(
        adhoc__master_dq_checks_passed, config.metastore_schema, context
    )
    yield Output(gold, metadata={"filepath": get_output_filepath(context)})
