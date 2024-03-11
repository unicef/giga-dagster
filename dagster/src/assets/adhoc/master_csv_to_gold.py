import os
from io import BytesIO

import numpy as np
import pandas as pd
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import NullType
from src.data_quality_checks.utils import (
    aggregate_report_spark_df,
    dq_failed_rows as extract_dq_failed_rows,
    dq_passed_rows as extract_dq_passed_rows,
    extract_school_id_govt_duplicates,
    row_level_checks,
)
from src.sensors.base import FileConfig
from src.utils.adls import ADLSFileClient, get_output_filepath
from src.utils.logger import ContextLoggerWithLoguruFallback
from src.utils.schema import get_schema_columns
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
def adhoc__master_data_transforms(
    context: OpExecutionContext,
    adhoc__load_master_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> pd.DataFrame:
    logger = ContextLoggerWithLoguruFallback(context)

    s: SparkSession = spark.spark_session
    columns = get_schema_columns(s, config.metastore_schema)

    buffer = BytesIO(adhoc__load_master_csv)
    buffer.seek(0)
    df = pd.read_csv(buffer).fillna(np.nan).replace([np.nan], [None])
    for col, dtype in df.dtypes.items():
        if dtype == "object":
            df[col] = df[col].astype("string")

    sdf = s.createDataFrame(df)

    columns_to_add = {
        col.name: f.lit(None).cast(NullType())
        for col in columns
        if col.name not in sdf.columns
    }

    sdf = logger.passthrough(
        sdf.withColumns(columns_to_add), f"Added {len(columns_to_add)} missing columns"
    )

    sdf = logger.passthrough(
        extract_school_id_govt_duplicates(sdf), "Added row number transforms"
    )

    yield Output(sdf.toPandas(), metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__df_duplicates(
    context: OpExecutionContext,
    adhoc__master_data_transforms: sql.DataFrame,
) -> pd.DataFrame:
    df_duplicates = adhoc__master_data_transforms.where(
        adhoc__master_data_transforms.row_num != 1
    )
    df_duplicates = df_duplicates.drop("row_num")

    context.log.info(f"Duplicate school_id_govt: {df_duplicates.count()=}")
    yield Output(
        df_duplicates.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_data_quality_checks(
    context: OpExecutionContext,
    adhoc__master_data_transforms: sql.DataFrame,
    config: FileConfig,
) -> pd.DataFrame:
    logger = ContextLoggerWithLoguruFallback(context)

    filepath = config.filepath
    filename = filepath.split("/")[-1]
    file_stem = os.path.splitext(filename)[0]
    country_iso3 = file_stem.split("_")[0]

    df_deduplicated = adhoc__master_data_transforms.where(
        adhoc__master_data_transforms.row_num == 1
    )
    df_deduplicated = df_deduplicated.drop("row_num")

    dq_checked = logger.passthrough(
        row_level_checks(df_deduplicated, "master", country_iso3, context),
        "Row level checks completed",
    )
    dq_checked = transform_types(dq_checked, config.metastore_schema, context)
    logger.log.info(
        f"Post-DQ checks stats: {len(df_deduplicated.columns)=}\n{df_deduplicated.count()=}"
    )
    yield Output(
        dq_checked.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_dq_checks_passed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
) -> pd.DataFrame:
    dq_passed = extract_dq_passed_rows(adhoc__master_data_quality_checks, "master")
    context.log.info(
        f"Extract passing rows: {len(dq_passed.columns)=}, {dq_passed.count()=}"
    )
    yield Output(
        dq_passed.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_dq_checks_failed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
) -> sql.DataFrame:
    dq_failed = extract_dq_failed_rows(adhoc__master_data_quality_checks, "master")
    context.log.info(
        f"Extract failed rows: {len(dq_failed.columns)=}, {dq_failed.count()=}"
    )
    yield Output(
        dq_failed.toPandas(), metadata={"filepath": get_output_filepath(context)}
    )


@asset(io_manager_key="adls_json_io_manager")
def adhoc__master_dq_checks_summary(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
    spark: PySparkResource,
) -> dict | list[dict]:
    df_summary = aggregate_report_spark_df(
        spark.spark_session, adhoc__master_data_quality_checks
    )
    yield Output(
        df_summary.toPandas().to_dict(orient="records"),
        metadata={"filepath": get_output_filepath(context)},
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
