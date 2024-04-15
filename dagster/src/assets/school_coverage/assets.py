from io import BytesIO

import pandas as pd
from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from icecream import ic
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import SparkSession
from sqlalchemy import select
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.internal.common_assets.staging import staging_step
from src.resources import ResourceKey
from src.schemas.file_upload import FileUploadConfig
from src.spark.coverage_transform_functions import (
    fb_coverage_merge,
    fb_transforms,
    itu_coverage_merge,
    itu_transforms,
)
from src.spark.transform_functions import (
    add_missing_columns,
    column_mapping_rename,
)
from src.utils.adls import ADLSFileClient
from src.utils.datahub.create_validation_tab import (
    datahub_emit_assertions_with_exception_catcher,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db import get_db_context
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.pandas import pandas_loader
from src.utils.schema import get_schema_columns, get_schema_columns_datahub

from dagster import OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
def coverage_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    config: FileConfig,
    spark: PySparkResource,
) -> bytes:
    df = adls_file_client.download_raw(config.filepath)

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )
    yield Output(df, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def coverage_data_quality_results(
    context,
    config: FileConfig,
    coverage_raw: bytes,
    spark: PySparkResource,
) -> pd.DataFrame:
    s: SparkSession = spark.spark_session

    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id),
        )
        if file_upload is None:
            raise FileNotFoundError(
                f"Database entry for FileUpload with id `{config.filename_components.id}` was not found",
            )

        file_upload = FileUploadConfig.from_orm(file_upload)

    with BytesIO(coverage_raw) as buffer:
        buffer.seek(0)
        pdf = pandas_loader(buffer, config.filepath)

    source = config.filename_components.source

    df_raw = s.createDataFrame(pdf)
    df, column_mapping = column_mapping_rename(
        df_raw,
        file_upload.column_to_schema_mapping,
    )
    columns = get_schema_columns(s, f"coverage_{source}")
    df = add_missing_columns(df, columns)
    dq_results = row_level_checks(
        df,
        f"coverage_{source}",
        config.filename_components.country_code,
        context,
    )

    config.metadata.update({"column_mapping": column_mapping})

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )

    dq_pandas = dq_results.toPandas()
    yield Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "column_mapping": column_mapping,
            "preview": get_table_preview(dq_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
def coverage_data_quality_results_summary(
    context,
    config: FileConfig,
    coverage_raw: bytes,
    coverage_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
) -> dict | list[dict]:
    s: SparkSession = spark.spark_session

    with BytesIO(coverage_raw) as buffer:
        buffer.seek(0)
        pdf = pandas_loader(buffer, config.filepath)

    df_raw = s.createDataFrame(pdf)
    dq_summary_statistics = aggregate_report_json(
        aggregate_report_spark_df(spark.spark_session, coverage_data_quality_results),
        df_raw,
    )

    datahub_emit_assertions_with_exception_catcher(
        context=context, dq_summary_statistics=dq_summary_statistics
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )

    yield Output(dq_summary_statistics, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def coverage_dq_passed_rows(
    context: OpExecutionContext,
    coverage_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> sql.DataFrame:
    df_passed = dq_split_passed_rows(coverage_data_quality_results, config.dataset_type)

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

    df_pandas = df_passed.toPandas()
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def coverage_dq_failed_rows(
    context: OpExecutionContext,
    coverage_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> sql.DataFrame:
    df_failed = dq_split_failed_rows(coverage_data_quality_results, config.dataset_type)

    schema_reference = get_schema_columns_datahub(
        spark.spark_session,
        config.metastore_schema,
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
        df_failed=df_failed,
    )

    df_pandas = df_failed.toPandas()
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
def coverage_bronze(
    context: OpExecutionContext,
    coverage_dq_passed_rows: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> sql.DataFrame:
    s: SparkSession = spark.spark_session
    source = ic(config.filename_components.source)
    silver_table_name = config.filename_components.country_code.lower()
    full_silver_table_name = f"{config.metastore_schema}.{silver_table_name}"

    if source == "fb":
        df = fb_transforms(coverage_dq_passed_rows)
    elif source == "itu":  # source == "itu"
        df = itu_transforms(coverage_dq_passed_rows)
    else:
        columns = get_schema_columns(s, config.metastore_schema)
        df = add_missing_columns(coverage_dq_passed_rows, columns)
        df = df.select(*[c.name for c in columns])

    if s.catalog.tableExists(full_silver_table_name):
        silver = DeltaTable.forName(spark.spark_session, full_silver_table_name).toDF()
        if source == "fb":
            df = fb_coverage_merge(df, silver)
        elif source == "itu":
            df = itu_coverage_merge(df, silver)

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

    df_pandas = df.toPandas()
    yield Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
def coverage_staging(
    context: OpExecutionContext,
    coverage_bronze: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
):
    staging = staging_step(
        context,
        config,
        adls_file_client,
        spark.spark_session,
        upstream_df=coverage_bronze,
    )

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
        None,
        metadata={
            **get_output_metadata(config),
            "preview": get_table_preview(staging),
        },
    )
