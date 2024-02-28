import json
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
from src.utils.logger import ContextLoggerWithLoguruFallback
from src.utils.spark import transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key="adls_passthrough_io_manager")
def adhoc__load_master_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    df_pandas = adls_file_client.download_csv_as_pandas_dataframe(
        context.run_tags["dagster/run_key"]
    )
    df_pandas = df_pandas.fillna(np.nan).replace([np.nan], [None])
    schema_name = config.metastore_schema
    schema_class: BaseSchema = getattr(src.schemas, schema_name)
    spark_schema = schema_class.schema

    df_spark = adls_file_client.download_csv_as_spark_dataframe(
        context.run_tags["dagster/run_key"],
        spark.spark_session,
        schema=spark_schema,
    )
    adls_file_client.upload_pandas_dataframe_as_file(
        df_pandas, context.run_tags["dagster/run_key"]
    )

    if len(df_pandas.columns) == len(df_spark.columns):
        context.log.info("Column count matches")
    else:
        context.log.warning(
            f"Column count mismatch: pandas={len(df_pandas.columns)}, spark={len(df_spark.columns)}"
        )

    if len(df_pandas) == df_spark.count():
        context.log.info("Row count matches")
    else:
        raise Exception(
            f"Row count mismatch: pandas={len(df_pandas)}, spark={df_spark.count()}"
        )

    if (
        df_pandas.isnull().sum().to_dict()
        == df_spark.toPandas().isnull().sum().to_dict()
    ):
        context.log.info("Null count matches")
    else:
        pandas_mismatch = df_pandas.isnull().sum().to_dict()
        spark_mismatch = df_spark.toPandas().isnull().sum().to_dict()
        dump_fn = lambda x: json.dumps(x, indent=2)  # noqa: E731
        context.log.warning(
            f"Null count mismatch:\npandas={dump_fn(pandas_mismatch)}\nspark={dump_fn(spark_mismatch)}"
        )

    yield Output(raw, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="spark_csv_io_manager")
def adhoc__master_data_quality_checks(
    context: OpExecutionContext,
    adhoc__load_master_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
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

    columns_to_rename = {
        col: adhoc_utils.COLUMN_RENAME_MAPPING[col]
        for col in sdf.columns
        if col in adhoc_utils.COLUMN_RENAME_MAPPING.keys()
    }

    columns_to_add = {
        col.name: f.lit(None).cast(NullType())
        for col in schema.columns
        if col.name not in sdf.columns
    }

    sdf = logger.passthrough(
        sdf.withColumnsRenamed(columns_to_rename),
        f"Renamed {len(columns_to_rename)} columns",
    )
    sdf = logger.passthrough(
        sdf.withColumns(columns_to_add), f"Added {len(columns_to_add)} missing columns"
    )

    dq_checked = logger.passthrough(
        row_level_checks(sdf, "master", country_iso3, context),
        "Row level checks completed",
    )
    yield Output(dq_checked, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="spark_csv_io_manager")
def adhoc__master_dq_checks_passed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
) -> sql.DataFrame:
    dq_passed = extract_dq_passed_rows(adhoc__master_data_quality_checks, "master")
    yield Output(dq_passed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="spark_csv_io_manager")
def adhoc__master_dq_checks_failed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
) -> pd.DataFrame:
    dq_failed = extract_dq_failed_rows(adhoc__master_data_quality_checks, "master")
    yield Output(dq_failed, metadata={"filepath": get_output_filepath(context)})


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
