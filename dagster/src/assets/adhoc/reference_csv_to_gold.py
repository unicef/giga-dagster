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
    dq_split_failed_rows as extract_dq_failed_rows,
    dq_split_passed_rows as extract_dq_passed_rows,
    row_level_checks,
)
from src.resources import ResourceKey
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import get_schema_columns, get_schema_columns_datahub
from src.utils.spark import compute_row_hash, transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def adhoc__load_reference_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
    spark: PySparkResource,
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )
    yield Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_data_quality_checks(
    context: OpExecutionContext,
    adhoc__load_reference_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[pd.DataFrame]:
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
    return Output(
        dq_checked.toPandas(),
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(dq_checked),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_dq_checks_passed(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_data_quality_checks: sql.DataFrame,
) -> Output[pd.DataFrame]:
    dq_passed = extract_dq_passed_rows(
        adhoc__reference_data_quality_checks,
        "reference",
    )
    dq_passed = dq_passed.withColumn(
        "signature",
        f.sha2(f.concat_ws("|", *sorted(dq_passed.columns)), 256),
    )
    context.log.info(f"Calculated SHA256 signature for {dq_passed.count()} rows")
    df_pandas = dq_passed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_dq_checks_failed(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_data_quality_checks: sql.DataFrame,
) -> Output[pd.DataFrame]:
    dq_failed = extract_dq_failed_rows(
        adhoc__reference_data_quality_checks,
        "reference",
    )
    return Output(
        dq_failed.toPandas(),
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(dq_failed.toPandas()),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__publish_reference_to_gold(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_dq_checks_passed: sql.DataFrame,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
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
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    return Output(
        gold,
        metadata={**get_output_metadata(config), "preview": get_table_preview(gold)},
    )
