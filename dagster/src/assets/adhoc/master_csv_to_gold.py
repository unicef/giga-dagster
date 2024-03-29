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
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows as extract_dq_failed_rows,
    dq_split_passed_rows as extract_dq_passed_rows,
    extract_school_id_govt_duplicates,
    row_level_checks,
)
from src.resources import ResourceKey
from src.utils.adls import ADLSFileClient
from src.utils.datahub.create_validation_tab import EmitDatasetAssertionResults
from src.utils.datahub.emit_dataset_metadata import (
    create_dataset_urn,
    emit_metadata_to_datahub,
)
from src.utils.logger import ContextLoggerWithLoguruFallback
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import get_schema_columns
from src.utils.spark import compute_row_hash, transform_types

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def adhoc__load_master_csv(
    context: OpExecutionContext, adls_file_client: ADLSFileClient, config: FileConfig
) -> bytes:
    raw = adls_file_client.download_raw(config.filepath)
    emit_metadata_to_datahub(
        context,
        df=raw,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    yield Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__master_data_transforms(
    context: OpExecutionContext,
    adhoc__load_master_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> pd.DataFrame:
    logger = ContextLoggerWithLoguruFallback(context)

    s: SparkSession = spark.spark_session
    columns = get_schema_columns(s, config.metastore_schema)

    with BytesIO(adhoc__load_master_csv) as buffer:
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

    df_pandas = sdf.toPandas()
    emit_metadata_to_datahub(
        context,
        df=df_pandas,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__df_duplicates(
    context: OpExecutionContext,
    adhoc__master_data_transforms: sql.DataFrame,
    config: FileConfig,
) -> pd.DataFrame:
    df_duplicates = adhoc__master_data_transforms.where(
        adhoc__master_data_transforms.row_num != 1
    )
    df_duplicates = df_duplicates.drop("row_num")

    context.log.info(f"Duplicate school_id_govt: {df_duplicates.count()=}")

    df_pandas = df_duplicates.toPandas()
    emit_metadata_to_datahub(
        context,
        df=df_pandas,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
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

    df_pandas = dq_checked.toPandas()
    emit_metadata_to_datahub(
        context,
        df=df_pandas,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__master_dq_checks_passed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
    config: FileConfig,
) -> pd.DataFrame:
    dq_passed = extract_dq_passed_rows(adhoc__master_data_quality_checks, "master")
    context.log.info(
        f"Extract passing rows: {len(dq_passed.columns)=}, {dq_passed.count()=}"
    )

    dq_passed = dq_passed.withColumn(
        "signature", f.sha2(f.concat_ws("|", *sorted(dq_passed.columns)), 256)
    )
    context.log.info(f"Calculated SHA256 signature for {dq_passed.count()} rows")

    df_pandas = dq_passed.toPandas()
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__master_dq_checks_failed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
    config: FileConfig,
) -> sql.DataFrame:
    dq_failed = extract_dq_failed_rows(adhoc__master_data_quality_checks, "master")
    context.log.info(
        f"Extract failed rows: {len(dq_failed.columns)=}, {dq_failed.count()=}"
    )

    df_pandas = dq_failed.toPandas()
    emit_metadata_to_datahub(
        context,
        df=df_pandas,
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
def adhoc__master_dq_checks_summary(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> dict | list[dict]:
    df_summary = aggregate_report_json(
        aggregate_report_spark_df(
            spark.spark_session, adhoc__master_data_quality_checks
        ),
        adhoc__master_data_quality_checks,
    )

    dataset_urn = create_dataset_urn(context, is_upstream=False)
    emit_assertions = EmitDatasetAssertionResults(
        dataset_urn=dataset_urn,
        dq_summary_statistics=df_summary,
        context=context,
    )
    emit_assertions()
    yield Output(df_summary, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_DELTA_V2_IO_MANAGER.value)
def adhoc__publish_master_to_gold(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__master_dq_checks_passed: sql.DataFrame,
) -> sql.DataFrame:
    gold = transform_types(
        adhoc__master_dq_checks_passed, config.metastore_schema, context
    )
    gold = compute_row_hash(gold)

    emit_metadata_to_datahub(
        context,
        df=gold.toPandas(),
        country_code=config.filename_components.country_code,
        dataset_urn=config.datahub_source_dataset_urn,
    )
    yield Output(
        gold,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(gold),
        },
    )
