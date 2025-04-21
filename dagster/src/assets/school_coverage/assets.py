from io import BytesIO

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
from src.internal.common_assets.staging import StagingChangeTypeEnum, StagingStep
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
from src.utils.data_quality_descriptions import (
    convert_dq_checks_to_human_readeable_descriptions_and_upload,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db.primary import get_db_context
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.pandas import pandas_loader
from src.utils.schema import (
    get_schema_columns,
    get_schema_columns_datahub,
)
from src.utils.send_email_dq_report import send_email_dq_report_with_config
from src.utils.sentry import capture_op_exceptions

from dagster import MetadataValue, OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
@capture_op_exceptions
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


@asset(io_manager_key=ResourceKey.INTERMEDIARY_ADLS_DELTA_IO_MANAGER.value)
@capture_op_exceptions
def coverage_data_quality_results(
    context: OpExecutionContext,
    config: FileConfig,
    coverage_raw: bytes,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
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
    country_code = config.country_code
    dataset_type = f"coverage_{source}"

    df_raw = s.createDataFrame(pdf)
    df, column_mapping = column_mapping_rename(
        df_raw,
        file_upload.column_to_schema_mapping,
    )
    columns = get_schema_columns(s, f"coverage_{source}")
    df = add_missing_columns(df, columns)
    dq_results = row_level_checks(
        df=df,
        dataset_type=dataset_type,
        _country_code_iso3=country_code,
        context=context,
    )

    config.metadata.update({"column_mapping": column_mapping})

    convert_dq_checks_to_human_readeable_descriptions_and_upload(
        dq_results=dq_results,
        bronze=df,
        dataset_type=dataset_type,
        config=config,
        context=context,
    )

    row_count = dq_results.count()
    preview_df = dq_results.limit(10).toPandas()

    return Output(
        dq_results,
        metadata={
            **get_output_metadata(config),
            "row_count": row_count,
            "column_mapping": column_mapping,
            "preview": get_table_preview(preview_df),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
@capture_op_exceptions
async def coverage_data_quality_results_summary(
    context: OpExecutionContext,
    config: FileConfig,
    coverage_raw: bytes,
    coverage_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
) -> Output[dict]:
    s: SparkSession = spark.spark_session

    with BytesIO(coverage_raw) as buffer:
        buffer.seek(0)
        pdf = pandas_loader(buffer, config.filepath)

    df_raw = s.createDataFrame(pdf)
    dq_summary_statistics = aggregate_report_json(
        df_aggregated=aggregate_report_spark_df(s, coverage_data_quality_results),
        df_bronze=df_raw,
        df_data_quality_checks=coverage_data_quality_results,
    )

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )

    await send_email_dq_report_with_config(
        dq_results=dq_summary_statistics,
        config=config,
        context=context,
    )

    return Output(dq_summary_statistics, metadata=get_output_metadata(config))


@asset(io_manager_key=ResourceKey.INTERMEDIARY_ADLS_DELTA_IO_MANAGER.value)
@capture_op_exceptions
def coverage_dq_passed_rows(
    context: OpExecutionContext,
    coverage_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
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

    row_count = df_passed.count()
    preview_df = df_passed.limit(10).toPandas()

    return Output(
        df_passed,
        metadata={
            **get_output_metadata(config),
            "row_count": row_count,
            "preview": get_table_preview(preview_df),
        },
    )


@asset(io_manager_key=ResourceKey.INTERMEDIARY_ADLS_DELTA_IO_MANAGER.value)
@capture_op_exceptions
def coverage_dq_failed_rows(
    context: OpExecutionContext,
    coverage_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
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

    row_count = df_failed.count()
    preview_df = df_failed.limit(10).toPandas()

    return Output(
        df_failed,
        metadata={
            **get_output_metadata(config),
            "row_count": row_count,
            "preview": get_table_preview(preview_df),
        },
    )


@asset(io_manager_key=ResourceKey.INTERMEDIARY_ADLS_DELTA_IO_MANAGER.value)
@capture_op_exceptions
def coverage_bronze(
    context: OpExecutionContext,
    coverage_dq_passed_rows: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    source = ic(config.filename_components.source)
    silver_table_name = config.country_code.lower()
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
        s.catalog.refreshTable(full_silver_table_name)
        silver = DeltaTable.forName(s, full_silver_table_name).toDF()
        if source == "fb":
            df = fb_coverage_merge(df, silver)
        elif source == "itu":
            df = itu_coverage_merge(df, silver)

    schema_reference = get_schema_columns_datahub(
        s,
        config.metastore_schema,
    )
    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=schema_reference,
    )

    row_count = df.count()
    preview_df = df.limit(10).toPandas()

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "row_count": row_count,
            "preview": get_table_preview(preview_df),
        },
    )


@asset
@capture_op_exceptions
def coverage_staging(
    context: OpExecutionContext,
    coverage_bronze: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
):
    if coverage_bronze.count() == 0:
        context.log.warning("Skipping staging as there are no passing bronze rows")
        return Output(None)

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
    staging_step = StagingStep(
        context,
        config,
        adls_file_client,
        spark.spark_session,
        StagingChangeTypeEnum.UPDATE,
    )
    staging = staging_step(coverage_bronze)
    row_count = 0 if staging is None else staging.count()
    preview_df = None if staging is None else staging.limit(10).toPandas()

    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "row_count": MetadataValue.int(row_count),
            "preview": get_table_preview(preview_df)
            if preview_df is not None
            else MetadataValue.text("No staging output"),
        },
    )


@asset
@capture_op_exceptions
def coverage_delete_staging(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[None]:
    delete_row_ids = adls_file_client.download_json(config.filepath)
    if isinstance(delete_row_ids, list):
        # dedupe change IDs
        delete_row_ids = list(set(delete_row_ids))

    staging_step = StagingStep(
        context,
        config,
        adls_file_client,
        spark.spark_session,
        StagingChangeTypeEnum.DELETE,
    )
    staging = staging_step(delete_row_ids)

    if staging is not None:
        datahub_emit_metadata_with_exception_catcher(
            context=context,
            config=config,
            spark=spark,
        )
        return Output(
            None,
            metadata={
                **get_output_metadata(config),
                "row_count": staging.count(),
                "preview": get_table_preview(staging),
                "delete_row_ids": MetadataValue.json(delete_row_ids),
            },
        )

    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "row_count": 0,
            "delete_row_ids": MetadataValue.json(delete_row_ids),
        },
    )
