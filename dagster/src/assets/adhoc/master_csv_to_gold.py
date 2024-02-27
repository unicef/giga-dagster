import json
from io import BytesIO

import numpy as np
import pandas as pd
import src.schemas
from dagster_pyspark import PySparkResource
from icecream import ic
from pyspark import sql
from pyspark.sql import SparkSession
from src.schemas import BaseSchema
from src.sensors.config import FileConfig
from src.utils.adls import ADLSFileClient, get_output_filepath

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


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_data_quality_checks(
    context: OpExecutionContext,
    adhoc__load_master_csv: bytes,
) -> pd.DataFrame:
    buffer = BytesIO(adhoc__load_master_csv)
    buffer.seek(0)
    dq_checked = pd.read_csv(buffer)
    yield Output(dq_checked, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_dq_checks_passed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
) -> pd.DataFrame:
    dq_passed = adhoc__master_data_quality_checks.toPandas()
    yield Output(dq_passed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_pandas_io_manager")
def adhoc__master_dq_checks_failed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
) -> pd.DataFrame:
    dq_failed = adhoc__master_data_quality_checks.toPandas()
    yield Output(dq_failed, metadata={"filepath": get_output_filepath(context)})


@asset(io_manager_key="adls_delta_v2_io_manager")
def adhoc__publish_master_to_gold(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    adhoc__master_dq_checks_passed: sql.DataFrame,
) -> sql.DataFrame:
    schema_name = ic(config.metastore_schema)
    schema_class: BaseSchema = getattr(src.schemas, schema_name)
    spark_schema = ic(schema_class.schema)
    s: SparkSession = spark.spark_session

    df = (
        adhoc__master_dq_checks_passed.toPandas()
        .fillna(np.nan)
        .replace([np.nan], [None])
    )
    gold = s.createDataFrame(df, schema=spark_schema)
    yield Output(gold, metadata={"filepath": get_output_filepath(context)})
