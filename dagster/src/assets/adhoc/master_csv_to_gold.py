import os
from io import BytesIO

import numpy as np
import pandas as pd
from country_converter import CountryConverter
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import NullType, StructType
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows as extract_dq_failed_rows,
    dq_split_passed_rows as extract_dq_passed_rows,
    extract_school_id_govt_duplicates,
    row_level_checks,
)
from src.internal.common_assets.master_release_notes import (
    send_master_release_notes,
)
from src.resources import ResourceKey
from src.settings import DeploymentEnvironment, settings
from src.spark.transform_functions import (
    add_missing_columns,
    connectivity_rt_dataset,
    merge_connectivity_to_master,
)
from src.utils.adls import ADLSFileClient
from src.utils.datahub.create_validation_tab import (
    datahub_emit_assertions_with_exception_catcher,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.logger import ContextLoggerWithLoguruFallback
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.schema import get_schema_columns, get_schema_columns_datahub
from src.utils.spark import compute_row_hash, transform_types

from azure.core.exceptions import ResourceNotFoundError
from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def adhoc__load_master_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[bytes]:
    raw = adls_file_client.download_raw(config.filepath)
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )
    return Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def adhoc__load_reference_csv(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[bytes]:
    try:
        raw = adls_file_client.download_raw(config.filepath)
    except ResourceNotFoundError as e:
        context.log.warning(
            f"Skipping due to no reference data found: {config.filepath}\n{e}"
        )
        return Output(b"")

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )
    return Output(raw, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__master_data_transforms(
    context: OpExecutionContext,
    adhoc__load_master_csv: bytes,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[pd.DataFrame]:
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
        sdf.withColumns(columns_to_add),
        f"Added {len(columns_to_add)} missing columns",
    )
    sdf = sdf.select([col.name for col in columns])

    sdf = logger.passthrough(
        extract_school_id_govt_duplicates(sdf),
        "Added row number transforms",
    )

    df_pandas = sdf.toPandas()
    return Output(
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
) -> Output[pd.DataFrame]:
    df_duplicates = adhoc__master_data_transforms.where(f.col("row_num") != 1)
    df_duplicates = df_duplicates.drop("row_num")

    context.log.info(f"Duplicate school_id_govt: {df_duplicates.count()=}")

    df_pandas = df_duplicates.toPandas()
    return Output(
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
) -> Output[pd.DataFrame]:
    logger = ContextLoggerWithLoguruFallback(context)

    filepath = config.filepath
    filename = filepath.split("/")[-1]
    file_stem = os.path.splitext(filename)[0]
    country_iso3 = file_stem.split("_")[0]

    df_deduplicated = adhoc__master_data_transforms.where(f.col("row_num") == 1)
    df_deduplicated = df_deduplicated.drop("row_num")

    dq_checked = logger.passthrough(
        row_level_checks(df_deduplicated, "master", country_iso3, context),
        "Row level checks completed",
    )
    dq_checked = transform_types(dq_checked, config.metastore_schema, context)
    logger.log.info(
        f"Post-DQ checks stats: {len(df_deduplicated.columns)=}\n{df_deduplicated.count()=}",
    )

    df_pandas = dq_checked.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_data_quality_checks(
    context: OpExecutionContext,
    spark: PySparkResource,
    config: FileConfig,
    adhoc__load_reference_csv: bytes,
) -> Output[pd.DataFrame]:
    if len(adhoc__load_reference_csv) == 0:
        return Output(pd.DataFrame())

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

    columns_non_nullable = ["education_level_govt", "school_id_govt_type"]
    column_actions = {
        c: f.coalesce(f.col(c), f.lit("Unknown")) for c in columns_non_nullable
    }

    sdf = sdf.withColumns(columns_to_add)
    sdf = sdf.withColumns(column_actions)
    sdf = sdf.select([col.name for col in columns])
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
def adhoc__master_dq_checks_passed(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
    config: FileConfig,
) -> Output[pd.DataFrame]:
    dq_passed = extract_dq_passed_rows(adhoc__master_data_quality_checks, "master")
    context.log.info(
        f"Extract passing rows: {len(dq_passed.columns)=}, {dq_passed.count()=}",
    )
    df_pandas = dq_passed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_dq_checks_passed(
    _: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_data_quality_checks: sql.DataFrame,
) -> Output[pd.DataFrame]:
    if adhoc__reference_data_quality_checks.isEmpty():
        return Output(pd.DataFrame())

    dq_passed = extract_dq_passed_rows(
        adhoc__reference_data_quality_checks,
        "reference",
    )
    df_pandas = dq_passed.toPandas()
    return Output(
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
) -> Output[pd.DataFrame]:
    dq_failed = extract_dq_failed_rows(adhoc__master_data_quality_checks, "master")
    context.log.info(
        f"Extract failed rows: {len(dq_failed.columns)=}, {dq_failed.count()=}",
    )

    df_pandas = dq_failed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def adhoc__reference_dq_checks_failed(
    _: OpExecutionContext,
    config: FileConfig,
    adhoc__reference_data_quality_checks: sql.DataFrame,
) -> Output[pd.DataFrame]:
    if adhoc__reference_data_quality_checks.isEmpty():
        return Output(pd.DataFrame())

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


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
def adhoc__master_dq_checks_summary(
    context: OpExecutionContext,
    adhoc__master_data_quality_checks: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> dict | list[dict]:
    df_summary = aggregate_report_json(
        aggregate_report_spark_df(
            spark.spark_session,
            adhoc__master_data_quality_checks,
        ),
        adhoc__master_data_quality_checks,
    )
    datahub_emit_assertions_with_exception_catcher(
        context=context, dq_summary_statistics=df_summary
    )

    yield Output(df_summary, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__publish_silver_geolocation(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    adhoc__master_dq_checks_passed: sql.DataFrame,
    adhoc__reference_dq_checks_passed: sql.DataFrame,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    schema_name = "school_geolocation"
    schema_columns = get_schema_columns(s, schema_name)
    column_names = [c.name for c in schema_columns]
    master_columns = [
        c
        for c in column_names
        if c in adhoc__master_dq_checks_passed.schema.fieldNames()
    ]

    df_one_gold = adhoc__master_dq_checks_passed.select(*master_columns)
    if not adhoc__reference_dq_checks_passed.isEmpty():
        reference_columns = [
            c
            for c in column_names
            if c in adhoc__reference_dq_checks_passed.schema.fieldNames()
        ]
        df_one_gold = df_one_gold.join(
            adhoc__reference_dq_checks_passed.select(*reference_columns),
            "school_id_giga",
            "left",
        )

    df_silver = add_missing_columns(df_one_gold, schema_columns)
    columns_non_nullable = [
        "school_id_govt_type",
        "education_level_govt",
    ]
    column_actions = {
        c: f.coalesce(f.col(c), f.lit("Unknown")) for c in columns_non_nullable
    }
    df_silver = df_silver.withColumns(column_actions)
    df_silver = transform_types(df_silver, schema_name, context)
    df_silver = compute_row_hash(df_silver, context)

    schema_reference = get_schema_columns_datahub(
        spark.spark_session,
        schema_name,
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    return Output(
        df_silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_silver),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__publish_silver_coverage(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    adhoc__master_dq_checks_passed: sql.DataFrame,
    adhoc__reference_dq_checks_passed: sql.DataFrame,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    schema_name = "school_coverage"
    schema_columns = get_schema_columns(s, schema_name)
    column_names = [c.name for c in schema_columns]
    master_columns = [
        c
        for c in column_names
        if c in adhoc__master_dq_checks_passed.schema.fieldNames()
    ]

    df_one_gold = adhoc__master_dq_checks_passed.select(*master_columns)
    if not adhoc__reference_dq_checks_passed.isEmpty():
        reference_columns = [
            c
            for c in column_names
            if c in adhoc__reference_dq_checks_passed.schema.fieldNames()
        ]
        df_one_gold = df_one_gold.join(
            adhoc__reference_dq_checks_passed.select(*reference_columns),
            "school_id_giga",
            "left",
        )

    df_silver = add_missing_columns(df_one_gold, schema_columns)
    columns_non_nullable = [
        "cellular_coverage_availability",
        "cellular_coverage_type",
    ]
    column_actions = {
        c: f.coalesce(f.col(c), f.lit("Unknown")) for c in columns_non_nullable
    }
    df_silver = df_silver.withColumns(column_actions)
    df_silver = transform_types(df_silver, schema_name, context)
    df_silver = compute_row_hash(df_silver, context)

    schema_reference = get_schema_columns_datahub(
        spark.spark_session,
        schema_name,
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    return Output(
        df_silver,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_silver),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__publish_master_to_gold(
    context: OpExecutionContext,
    config: FileConfig,
    adhoc__master_dq_checks_passed: sql.DataFrame,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
    gold = transform_types(
        adhoc__master_dq_checks_passed,
        config.metastore_schema,
        context,
    )

    if settings.DEPLOY_ENV != DeploymentEnvironment.LOCAL:
        # Connectivity db has IP blocks so doesn't work locally
        coco = CountryConverter()
        country_code_2 = coco.convert(config.country_code, to="ISO2")
        connectivity = connectivity_rt_dataset(spark.spark_session, country_code_2)
        gold = merge_connectivity_to_master(gold, connectivity)

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
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(gold),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def adhoc__publish_reference_to_gold(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    adhoc__reference_dq_checks_passed: sql.DataFrame,
) -> Output[sql.DataFrame]:
    if adhoc__reference_dq_checks_passed.isEmpty():
        s: SparkSession = spark.spark_session
        return Output(s.createDataFrame([], StructType()))

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


@asset
async def adhoc__broadcast_master_release_notes(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    adhoc__publish_master_to_gold: sql.DataFrame,
) -> Output[None]:
    metadata = await send_master_release_notes(
        context, config, spark, adhoc__publish_master_to_gold
    )
    if metadata is None:
        return Output(None)

    return Output(None, metadata=metadata)
