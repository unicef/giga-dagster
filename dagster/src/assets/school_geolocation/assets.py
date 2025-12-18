from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StringType, StructType
from sqlalchemy import select
from src.constants import DataTier, UploadMode
from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    aggregate_report_statistics,
    dq_geolocation_extract_relevant_columns,
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.internal.common_assets.staging import StagingChangeTypeEnum, StagingStep
from src.resources import ResourceKey
from src.schemas.file_upload import FileUploadConfig
from src.spark.config_expectations import config as config_expectations
from src.spark.transform_functions import (
    add_missing_columns,
    column_mapping_rename,
    create_bronze_layer_columns,
    standardize_connectivity_type,
)
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.data_quality_descriptions import (
    convert_dq_checks_to_human_readeable_descriptions_and_upload,
)
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db.primary import get_db_context
from src.utils.delta import check_table_exists, create_delta_table, create_schema
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig
from src.utils.pandas import pandas_loader
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_schema_columns,
    get_schema_columns_datahub,
    get_schema_table,
)
from src.utils.send_email_dq_report import send_email_dq_report_with_config
from src.utils.sentry import capture_op_exceptions
from src.utils.spark import spark_loader

from dagster import MetadataValue, OpExecutionContext, Output, asset


@asset(io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value)
@capture_op_exceptions
def geolocation_raw(
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


@asset
def geolocation_metadata(
    context: OpExecutionContext,
    geolocation_raw: bytes,
    config: FileConfig,
    spark: PySparkResource,
):
    s: SparkSession = spark.spark_session

    context.log.info("Get upload details")
    file_size_bytes = config.file_size_bytes
    metadata = config.metadata
    file_path = config.filepath
    country_code = config.country_code
    schema_name = config.metastore_schema
    file_name = Path(file_path).name
    giga_sync_id = file_name.split("_")[0]
    giga_sync_uploaded_at = datetime.strptime(
        file_name.split(".")[0].split("_")[-1], "%Y%m%d-%H%M%S"
    )

    upload_details = {
        "giga_sync_id": giga_sync_id,
        "country_code": country_code,
        "country": country_code,  # Alias for country_code to satisfy NOT NULL constraint
        "giga_sync_uploaded_at": giga_sync_uploaded_at,
        "schema_name": schema_name,
        "raw_file_path": file_path,
        "file_size_bytes": file_size_bytes,
    }

    context.log.info("Create upload details dataframe")
    df = pd.DataFrame([upload_details])

    context.log.info("Create giga sync metadata dataframe")
    metadata_df = pd.DataFrame([metadata])

    context.log.info("Combine dataframes")
    metadata_df = pd.concat([df, metadata_df], axis="columns")
    metadata_df["created_at"] = pd.Timestamp.now()

    context.log.info("Create spark dataframe")
    metadata_df = s.createDataFrame(metadata_df)

    table_columns = get_schema_columns(s, "school_geolocation_metadata")
    table_name = "school_geolocation_metadata"
    table_schema_name = "pipeline_tables"

    context.log.info("Create the schema and table if they do not exist")
    metadata_df = add_missing_columns(metadata_df, table_columns)
    metadata_df = metadata_df.select(*StructType(table_columns).fieldNames())

    create_schema(s, table_schema_name)
    create_delta_table(
        s,
        table_schema_name,
        table_name,
        table_columns,
        context,
        if_not_exists=True,
    )

    context.log.info("Upsert the metadata from giga sync into the table")
    s.catalog.refreshTable(construct_full_table_name(table_schema_name, table_name))
    current_metadata_table = DeltaTable.forName(
        s, construct_full_table_name(table_schema_name, table_name)
    )

    (
        current_metadata_table.alias("metadata_current")
        .merge(
            metadata_df.alias("metadata_updates"),
            "metadata_current.giga_sync_id = metadata_updates.giga_sync_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    context.log.info("Upsert operation completed")

    return Output(None)


@asset(io_manager_key=ResourceKey.ADLS_DELTA_TABLE_IO_MANAGER.value)
@capture_op_exceptions
def geolocation_bronze(
    context: OpExecutionContext,
    geolocation_raw: bytes,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    country_code = config.country_code
    schema_name = config.metastore_schema
    mode = config.metadata.get("mode", UploadMode.UPDATE.value)

    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id),
        )
        if file_upload is None:
            raise FileNotFoundError(
                f"Database entry for FileUpload with id `{config.filename_components.id}` was not found",
            )

        file_upload = FileUploadConfig.from_orm(file_upload)

    # Check if it's an Excel file that needs raw bytes
    from pathlib import Path

    ext = Path(config.filepath).suffix.lower()

    if ext in [".xlsx", ".xls"]:
        # Excel files require raw_bytes parameter
        df = spark_loader(s, config.filepath, raw_bytes=geolocation_raw)
    else:
        # Non-Excel files can be loaded directly from ADLS
        df = spark_loader(s, config.filepath)

    # Log partition count for monitoring parallelization
    context.log.info(f"Loaded DataFrame with {df.rdd.getNumPartitions()} partitions")

    df, column_mapping = column_mapping_rename(df, file_upload.column_to_schema_mapping)
    context.log.info("COLUMN MAPPING")
    context.log.info(column_mapping)
    context.log.info("COLUMN MAPPING DATAFRAME")
    context.log.info(df)
    uploaded_columns = df.columns

    columns = get_schema_columns(s, schema_name)
    context.log.info("schema columns")
    context.log.info(columns)

    schema = StructType(columns)

    # Create empty base schema DataFrame
    geolocation_base = s.createDataFrame(s.sparkContext.emptyRDD(), schema=schema)

    casted_geolocation_base = geolocation_base.withColumn(
        "school_id_govt", f.col("school_id_govt").cast(StringType())
    )

    context.log.info("Casted Geolocation")
    context.log.info(casted_geolocation_base)

    casted_bronze = df.withColumn(
        "school_id_govt", f.col("school_id_govt").cast(StringType())
    )

    context.log.info("Casted Bronze")
    context.log.info(casted_bronze)

    df = create_bronze_layer_columns(
        casted_bronze, casted_geolocation_base, country_code, mode, uploaded_columns
    )
    context.log.info("DF from create_bronze_layer_columns")
    context.log.info(df)

    config.metadata.update({"column_mapping": column_mapping})
    context.log.info("After config metadata update")

    # TEMPORARILY DISABLED FOR TESTING - connectivity feature not in original branch
    # if settings.DEPLOY_ENV != DeploymentEnvironment.LOCAL:
    #     # RT Columns
    #     connectivity = get_country_rt_schools(s, country_code)
    #     df = merge_connectivity_to_df(df, connectivity, uploaded_columns, mode)

    # standardize the connectivity type
    df = standardize_connectivity_type(df, mode, uploaded_columns)

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=df,
    )

    for column in config_expectations.TITLE_CASE_COLUMNS:
        if column in df.columns:
            df = df.withColumn(column, f.initcap(f.col(column)))

    # OPTIMIZATION: Cache the DataFrame before multiple actions
    # This prevents re-computation for count() and limit().toPandas()
    df.cache()
    row_count = df.count()
    preview_df = df.limit(10).toPandas()

    # The cached DataFrame will be used by the IO manager for the write
    # It will be unpersisted after the write completes

    return Output(
        df,
        metadata={
            **get_output_metadata(config),
            "row_count": row_count,
            "column_mapping": column_mapping,
            "preview": get_table_preview(preview_df),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_DELTA_TABLE_IO_MANAGER.value)
@capture_op_exceptions
def geolocation_data_quality_results(
    context: OpExecutionContext,
    config: FileConfig,
    geolocation_bronze: sql.DataFrame,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
    s: SparkSession = spark.spark_session
    country_code = config.country_code
    schema_name = config.metastore_schema
    dataset_type = "geolocation"

    columns = get_schema_columns(s, schema_name)
    schema = StructType(columns)

    if check_table_exists(s, schema_name, country_code, DataTier.SILVER):
        silver_tier_schema_name = construct_schema_name_for_tier(
            "school_geolocation", DataTier.SILVER
        )
        silver_table_name = construct_full_table_name(
            silver_tier_schema_name, country_code
        )
        s.catalog.refreshTable(silver_table_name)
        silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    else:
        silver = s.createDataFrame(s.sparkContext.emptyRDD(), schema=schema)

    casted_silver = silver.withColumn(
        "school_id_govt", f.col("school_id_govt").cast(StringType())
    )
    casted_bronze = geolocation_bronze.withColumn(
        "school_id_govt", f.col("school_id_govt").cast(StringType())
    )

    renamed_bronze = casted_bronze.withColumnRenamed("signature", "dq_signature")

    dq_results = row_level_checks(
        df=renamed_bronze,
        silver=casted_silver,
        dataset_type=dataset_type,
        _country_code_iso3=country_code,
        mode=config.metadata.get("mode", UploadMode.UPDATE.value),
        context=context,
    )

    dq_results = dq_results.withColumnRenamed("dq_signature", "signature")

    convert_dq_checks_to_human_readeable_descriptions_and_upload(
        dq_results=dq_results,
        dataset_type=dataset_type,
        bronze=casted_bronze,
        config=config,
        context=context,
    )

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )

    row_count = dq_results.count()
    preview_df = dq_results.limit(10).toPandas()

    return Output(
        dq_results,
        metadata={
            **get_output_metadata(config),
            "row_count": row_count,
            "preview": get_table_preview(preview_df),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
async def geolocation_data_quality_results_human_readable(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
) -> Output[pd.DataFrame]:
    context.log.info("Get the file upload object from the database")
    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id),
        )
        if file_upload is None:
            raise FileNotFoundError(
                f"Database entry for FileUpload with id `{config.filename_components.id}` was not found",
            )

    context.log.info("Obtain the list of uploaded columns")
    file_upload = FileUploadConfig.from_orm(file_upload)
    column_mapping = file_upload.column_to_schema_mapping
    uploaded_columns = list(column_mapping.values())
    context.log.info(f"The list of uploaded columns is: {uploaded_columns}")
    mode = config.metadata.get("mode", UploadMode.UPDATE.value)

    context.log.info("Create a new dataframe with only the relevant columns")
    df, human_readable_mappings = dq_geolocation_extract_relevant_columns(
        geolocation_data_quality_results, uploaded_columns, mode
    )
    # replace the dq_column column binary values with Yes/No depending on if they passed or failed the check
    dq_column_names = [
        col
        for col in df.columns
        if (col.startswith("dq_") and col != "dq_has_critical_error")
    ]
    for column in dq_column_names:
        df = df.withColumn(
            column,
            f.when(f.col(column) == 1, "No").otherwise(
                f.when(f.col(column) == 0, "Yes")
            ),
        )

    df = df.withColumnsRenamed(human_readable_mappings)
    context.log.info("Convert the dataframe to a pandas object to save it locally")
    df_pandas = df.toPandas()

    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df_pandas),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
async def geolocation_dq_schools_passed_human_readable(
    context: OpExecutionContext,
    geolocation_data_quality_results_human_readable: sql.DataFrame,
    config: FileConfig,
) -> Output[pd.DataFrame]:
    context.log.info("Filter and keep schools that do not have a critical error")
    df = geolocation_data_quality_results_human_readable.filter(
        geolocation_data_quality_results_human_readable.dq_has_critical_error == 0
    )
    df = df.drop("dq_has_critical_error", "failure_reason")
    df_pandas = df.toPandas()

    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df_pandas),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
async def geolocation_dq_schools_failed_human_readable(
    context: OpExecutionContext,
    geolocation_data_quality_results_human_readable: sql.DataFrame,
    config: FileConfig,
) -> Output[pd.DataFrame]:
    context.log.info("Filter and keep schools that have a critical error")
    df = geolocation_data_quality_results_human_readable.filter(
        geolocation_data_quality_results_human_readable.dq_has_critical_error == 1
    )

    df = df.drop("dq_has_critical_error")
    df_pandas = df.toPandas()

    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df_pandas),
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value)
@capture_op_exceptions
async def geolocation_data_quality_results_summary(
    context: OpExecutionContext,
    geolocation_bronze: sql.DataFrame,
    geolocation_data_quality_results: sql.DataFrame,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[dict]:
    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id),
        )
        if file_upload is None:
            raise FileNotFoundError(
                f"Database entry for FileUpload with id `{config.filename_components.id}` was not found",
            )
    file_upload = FileUploadConfig.from_orm(file_upload)
    column_mapping = file_upload.column_to_schema_mapping
    uploaded_columns = list(column_mapping.values())
    mode = config.metadata.get("mode", UploadMode.UPDATE.value)
    context.log.info(f"The list of uploaded columns is: {uploaded_columns}")

    dq_results, _ = dq_geolocation_extract_relevant_columns(
        geolocation_data_quality_results, uploaded_columns, mode=mode
    )

    dq_summary_statistics = aggregate_report_json(
        df_aggregated=aggregate_report_spark_df(
            spark.spark_session,
            dq_results,
        ),
        df_bronze=geolocation_bronze,
        df_data_quality_checks=dq_results,
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


@asset(io_manager_key=ResourceKey.ADLS_GENERIC_FILE_IO_MANAGER.value)
def geolocation_data_quality_report(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    geolocation_raw: bytes,
    config: FileConfig,
    spark: PySparkResource,
):
    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id),
        )
        if file_upload is None:
            raise FileNotFoundError(
                f"Database entry for FileUpload with id `{config.filename_components.id}` was not found",
            )

        file_upload = FileUploadConfig.from_orm(file_upload)

    with BytesIO(geolocation_raw) as buffer:
        buffer.seek(0)
        original_df = pandas_loader(buffer, config.filepath, context=context).map(str)

    original_df_columns = original_df.columns
    uploaded_columns = file_upload.column_to_schema_mapping.values()

    uploaded_columns_not_used = list(set(original_df_columns) - set(uploaded_columns))

    schema = get_schema_table(spark.spark_session, config.metastore_schema)
    important_columns_df = schema.filter(f.col("is_important"))
    important_columns_list = [
        row[0] for row in important_columns_df.select("name").collect()
    ]

    important_columns_not_uploaded = list(
        set(important_columns_list) - set(uploaded_columns)
    )
    important_columns_not_uploaded = [
        col for col in important_columns_not_uploaded if not col.startswith("admin")
    ]

    upload_details = {
        "country_code": file_upload.country,
        "file_name": file_upload.original_filename,
        "uploaded_columns_not_used": uploaded_columns_not_used,
        "important_columns_not_uploaded": important_columns_not_uploaded,
    }

    dq_report = aggregate_report_statistics(
        geolocation_data_quality_results, upload_details
    )
    return Output(dq_report)


@asset(io_manager_key=ResourceKey.ADLS_DELTA_TABLE_IO_MANAGER.value)
@capture_op_exceptions
def geolocation_dq_passed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
    df_passed = dq_split_passed_rows(
        geolocation_data_quality_results,
        config.dataset_type,
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


@asset(io_manager_key=ResourceKey.ADLS_DELTA_TABLE_IO_MANAGER.value)
@capture_op_exceptions
def geolocation_dq_failed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[sql.DataFrame]:
    df_failed = dq_split_failed_rows(
        geolocation_data_quality_results,
        config.dataset_type,
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


@asset
@capture_op_exceptions
def geolocation_staging(
    context: OpExecutionContext,
    geolocation_dq_passed_rows: sql.DataFrame,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
    config: FileConfig,
) -> Output[None]:
    if geolocation_dq_passed_rows.count() == 0:
        context.log.warning("Skipping staging as there are no rows passing DQ checks")
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
    staging = staging_step(geolocation_dq_passed_rows)
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
def geolocation_delete_staging(
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
                "preview": get_table_preview(staging.limit(10).toPandas()),
                "delete_row_ids": MetadataValue.json(delete_row_ids),
            },
        )

    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "delete_row_ids": MetadataValue.json(delete_row_ids),
        },
    )
