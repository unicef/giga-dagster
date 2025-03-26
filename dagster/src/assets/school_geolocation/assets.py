from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
from country_converter import CountryConverter
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from models.file_upload import FileUpload
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import LongType, StringType, StructType
from sqlalchemy import select
from src.constants import DataTier
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
from src.settings import DeploymentEnvironment, settings
from src.spark.transform_functions import (
    add_missing_columns,
    column_mapping_rename,
    connectivity_rt_dataset,
    create_bronze_layer_columns,
    merge_connectivity_to_master as merge_connectivity_to_df,
    standardize_connectivity_type,
)
from src.utils.adls import (
    ADLSFileClient,
)
from src.utils.data_quality_descriptions import (
    convert_dq_checks_to_human_readeable_descriptions_and_upload,
)
from src.utils.datahub.create_validation_tab import (
    datahub_emit_assertions_with_exception_catcher,
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
)
from src.utils.send_email_dq_report import send_email_dq_report_with_config
from src.utils.sentry import capture_op_exceptions

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


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
def geolocation_bronze(
    context: OpExecutionContext,
    geolocation_raw: bytes,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session
    country_code = config.country_code
    schema_name = config.metastore_schema
    mode = config.metadata["mode"]

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
        pdf = pandas_loader(buffer, config.filepath).map(str)

    pdf.rename(lambda name: name.strip(), axis="columns", inplace=True)
    df = s.createDataFrame(pdf)
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
        "school_id_govt",
        f.when(
            f.col("school_id_govt").cast(LongType()).isNotNull(),
            f.col("school_id_govt").cast(LongType()).cast(StringType()),
        ).otherwise(f.col("school_id_govt").cast(StringType())),
    )

    context.log.info("Casted Geolocation")
    context.log.info(casted_geolocation_base)

    casted_bronze = df.withColumn(
        "school_id_govt",
        f.when(
            f.col("school_id_govt").cast(LongType()).isNotNull(),
            f.col("school_id_govt").cast(LongType()).cast(StringType()),
        ).otherwise(f.col("school_id_govt").cast(StringType())),
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

    if settings.DEPLOY_ENV != DeploymentEnvironment.LOCAL:
        # QoS Columns
        coco = CountryConverter()
        country_code_2 = coco.convert(country_code, to="ISO2")
        connectivity = connectivity_rt_dataset(s, country_code_2)
        df = merge_connectivity_to_df(df, connectivity, uploaded_columns, mode)

    # standardize the connectivity type
    if "connectivity_type_govt" in uploaded_columns:
        df = standardize_connectivity_type(df, mode)

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
        schema_reference=df,
    )

    ## at this point it's already gone
    context.log.info("BEFORE DF TO PANDAS")
    df_pandas = df.toPandas()
    context.log.info("AFTER DF TO PANDAS")
    context.log.info(df_pandas)

    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df_pandas),
            "column_mapping": column_mapping,
            "preview": get_table_preview(df_pandas),
        },
    )


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
def geolocation_data_quality_results(
    context: OpExecutionContext,
    config: FileConfig,
    geolocation_bronze: sql.DataFrame,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
    s: SparkSession = spark.spark_session
    country_code = config.country_code
    schema_name = config.metastore_schema
    id = config.filename_components.id
    dataset_type = "geolocation"

    current_timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    columns = get_schema_columns(s, schema_name)
    schema = StructType(columns)

    if check_table_exists(s, schema_name, country_code, DataTier.SILVER):
        silver_tier_schema_name = construct_schema_name_for_tier(
            "school_geolocation", DataTier.SILVER
        )
        silver_table_name = construct_full_table_name(
            silver_tier_schema_name, country_code
        )
        silver = DeltaTable.forName(s, silver_table_name).alias("silver").toDF()
    else:
        silver = s.createDataFrame(s.sparkContext.emptyRDD(), schema=schema)

    casted_silver = silver.withColumn(
        "school_id_govt",
        f.when(
            f.col("school_id_govt").cast(LongType()).isNotNull(),
            f.col("school_id_govt").cast(LongType()).cast(StringType()),
        ).otherwise(f.col("school_id_govt").cast(StringType())),
    )
    casted_bronze = geolocation_bronze.withColumn(
        "school_id_govt",
        f.when(
            f.col("school_id_govt").cast(LongType()).isNotNull(),
            f.col("school_id_govt").cast(LongType()).cast(StringType()),
        ).otherwise(f.col("school_id_govt").cast(StringType())),
    )

    renamed_bronze = casted_bronze.withColumnRenamed("signature", "dq_signature")

    dq_results = row_level_checks(
        df=renamed_bronze,
        silver=casted_silver,
        dataset_type=dataset_type,
        _country_code_iso3=country_code,
        mode=config.metadata["mode"],
        context=context,
    )

    dq_results = dq_results.withColumnRenamed("dq_signature", "signature")

    dq_results_schema_name = f"{schema_name}_dq_results"
    table_name = f"{id}_{country_code}_{current_timestamp}"

    schema_columns = dq_results.schema.fields
    for col in schema_columns:
        col.nullable = True

    dq_results_table_name = construct_full_table_name(
        dq_results_schema_name,
        table_name,
    )

    create_schema(s, dq_results_schema_name)
    create_delta_table(
        s,
        dq_results_schema_name,
        table_name,
        schema_columns,
        context,
        if_not_exists=True,
    )
    dq_results.write.format("delta").mode("append").saveAsTable(dq_results_table_name)

    convert_dq_checks_to_human_readeable_descriptions_and_upload(
        dq_results=dq_results,
        dataset_type=dataset_type,
        bronze=casted_bronze,
        config=config,
        context=context,
    )

    dq_pandas = dq_results.toPandas()

    datahub_emit_metadata_with_exception_catcher(
        context=context,
        config=config,
        spark=spark,
    )

    return Output(
        dq_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(dq_pandas),
            "preview": get_table_preview(dq_pandas),
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
    dq_summary_statistics = aggregate_report_json(
        df_aggregated=aggregate_report_spark_df(
            spark.spark_session,
            geolocation_data_quality_results,
        ),
        df_bronze=geolocation_bronze,
        df_data_quality_checks=geolocation_data_quality_results,
    )

    datahub_emit_assertions_with_exception_catcher(
        context=context, dq_summary_statistics=dq_summary_statistics
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


@asset(io_manager_key=ResourceKey.ADLS_PANDAS_IO_MANAGER.value)
@capture_op_exceptions
def geolocation_dq_passed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
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

    df_pandas = df_passed.toPandas()
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
def geolocation_dq_failed_rows(
    context: OpExecutionContext,
    geolocation_data_quality_results: sql.DataFrame,
    config: FileConfig,
    spark: PySparkResource,
) -> Output[pd.DataFrame]:
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

    df_pandas = df_failed.toPandas()
    return Output(
        df_pandas,
        metadata={
            **get_output_metadata(config),
            "row_count": len(df_pandas),
            "preview": get_table_preview(df_pandas),
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

    return Output(
        None,
        metadata={
            **get_output_metadata(config),
            "row_count": MetadataValue.int(row_count),
            "preview": get_table_preview(staging),
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
                "preview": get_table_preview(staging),
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
