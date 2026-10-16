from io import BytesIO
from pathlib import Path

import pandas as pd
from dagster import (
    AssetIn,
    AssetKey,
    AssetOut,
    OpExecutionContext,
    Output,
    asset,
    multi_asset,
)
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StringType, StructField, StructType
from sqlalchemy import select

from models.file_upload import FileUpload
from src.data_quality_checks.facility import (
    collapse_facility_dq_flags,
    prepare_facility_bronze,
    run_facility_geolocation_checks,
    split_facility_dq_rows,
)
from src.data_quality_checks.location_grouping import (
    MEMBER_IDENTITY_SCHEMA,
    attach_approval_status,
    combine_duplicate_members,
    finalize_duplicates_report,
)
from src.data_quality_checks.utils import (
    build_dq_summary_statistics,
    build_dq_warning_columns,
    dq_geolocation_extract_relevant_columns,
    normalize_dq_results_map,
)
from src.facilities import FacilityGeolocationSpec
from src.internal.common_assets.staging import StagingMode, StagingStep
from src.resources import ResourceKey
from src.schemas.file_upload import FileUploadConfig
from src.utils.adls import ADLSFileClient
from src.utils.aggregate_value_maps_for_pdf import aggregate_value_maps_for_pdf
from src.utils.datahub.emit_dataset_metadata import (
    datahub_emit_metadata_with_exception_catcher,
)
from src.utils.db.primary import get_db_context
from src.utils.delta import create_schema
from src.utils.metadata import (
    get_output_metadata,
    get_staging_change_type_metadata,
    get_table_preview,
)
from src.utils.op_config import FileConfig
from src.utils.pandas import pandas_loader
from src.utils.schema import construct_full_table_name
from src.utils.send_email_dq_report import send_email_dq_report_with_config
from src.utils.spark import normalize_missing_value_strings


def _asset_key(name: str) -> AssetKey:
    return AssetKey(name)


def _load_current_facilities(
    spark: SparkSession,
    spec: FacilityGeolocationSpec,
    country_code: str,
    fallback_schema: StructType,
) -> sql.DataFrame:
    core_table = construct_full_table_name(spec.tables.core_schema, country_code)
    extension_table = construct_full_table_name(
        spec.tables.extension_schema, country_code
    )
    if not spark.catalog.tableExists(core_table):
        return spark.createDataFrame([], fallback_schema)

    core = DeltaTable.forName(spark, core_table).toDF()
    if "facility_type" in core.columns:
        core = core.filter(f.col("facility_type") == spec.facility_type)
    if spark.catalog.tableExists(extension_table):
        extension = DeltaTable.forName(spark, extension_table).toDF()
        core = core.join(extension, on="facility_id_giga", how="left")

    # Convert canonical core columns back to the facility's ingestion vocabulary.
    for source, canonical in spec.core_aliases:
        if source != canonical and canonical in core.columns and source not in core.columns:
            core = core.withColumn(source, f.col(canonical))
    return core


def _uploaded_columns(config: FileConfig) -> list[str]:
    with get_db_context() as db:
        file_upload = db.scalar(
            select(FileUpload).where(FileUpload.id == config.filename_components.id)
        )
        if file_upload is None:
            raise FileNotFoundError(
                f"FileUpload `{config.filename_components.id}` was not found"
            )
        model = FileUploadConfig.from_orm(file_upload)
    return list(model.column_to_schema_mapping.values())


def _human_readable_rows(
    dq_results: sql.DataFrame,
    uploaded_columns: list[str],
    spec: FacilityGeolocationSpec,
) -> sql.DataFrame:
    selected, mappings = dq_geolocation_extract_relevant_columns(
        dq_results,
        uploaded_columns,
        dq_config_table=spec.dq_config_table,
        is_new_column="is_new_facility",
    )
    for map_key, label in mappings.items():
        selected = selected.withColumn(
            label,
            f.when(f.element_at(f.col("dq_results"), map_key) == 1, "No")
            .when(f.element_at(f.col("dq_results"), map_key) == 0, "Yes"),
        )
    return selected.drop("dq_results")


def build_facility_geolocation_assets(  # noqa: C901
    spec: FacilityGeolocationSpec,
) -> list:
    """Create the automated-check asset graph for one facility type."""
    prefix = spec.assets.raw.removesuffix("_raw")
    metadata_name = f"{prefix}_metadata"
    duplicates_name = f"{prefix}_dq_duplicates_report"
    human_passed_name = f"{prefix}_dq_passed_human_readable"
    human_failed_name = f"{prefix}_dq_failed_human_readable"
    summary_name = f"{prefix}_data_quality_results_summary"
    passed_name = f"{prefix}_dq_passed_rows"
    failed_name = f"{prefix}_dq_failed_rows"
    error_name = f"{prefix}_error_table"
    map_name = f"{prefix}_map"
    kit_name = f"{prefix}_dq_kit_zip"
    delete_name = f"{prefix}_delete_staging"

    @asset(
        name=spec.assets.raw,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_PASSTHROUGH_IO_MANAGER.value,
    )
    def facility_raw(
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

    @asset(
        name=metadata_name,
        group_name=spec.group_name,
        ins={"raw_data": AssetIn(key=_asset_key(spec.assets.raw))},
    )
    def facility_metadata(
        raw_data: bytes,
        config: FileConfig,
    ) -> Output[None]:
        # FileUpload remains the authoritative metadata record. This asset preserves
        # graph parity without creating another facility-specific metadata table.
        del raw_data
        _uploaded_columns(config)
        return Output(None, metadata=get_output_metadata(config))

    @asset(
        name=spec.assets.bronze,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_SPARK_IO_MANAGER.value,
        ins={"raw_data": AssetIn(key=_asset_key(spec.assets.raw))},
    )
    def facility_bronze(
        context: OpExecutionContext,
        raw_data: bytes,
        config: FileConfig,
        spark: PySparkResource,
    ) -> Output[sql.DataFrame]:
        session = spark.spark_session
        with get_db_context() as db:
            file_upload = db.scalar(
                select(FileUpload).where(
                    FileUpload.id == config.filename_components.id
                )
            )
            if file_upload is None:
                raise FileNotFoundError(
                    f"FileUpload `{config.filename_components.id}` was not found"
                )
            upload = FileUploadConfig.from_orm(file_upload)

        mapping = {
            source.strip(): target
            for source, target in upload.column_to_schema_mapping.items()
            if source and target
        }
        with BytesIO(raw_data) as buffer:
            pdf = pandas_loader(buffer, config.filepath, context=context)
        pdf.rename(lambda column: column.strip(), axis="columns", inplace=True)
        pdf = pdf[list(mapping)].rename(columns=mapping)
        pdf = pdf.map(lambda value: None if pd.isna(value) else str(value))
        raw_schema = StructType(
            [StructField(column, StringType(), True) for column in pdf.columns]
        )
        dataframe = normalize_missing_value_strings(
            session.createDataFrame(pdf, schema=raw_schema)
        )
        if spec.country_column not in dataframe.columns:
            dataframe = dataframe.withColumn(
                spec.country_column, f.lit(config.country_code)
            )
        current = _load_current_facilities(
            session,
            spec,
            config.country_code,
            dataframe.schema,
        )
        dataframe = prepare_facility_bronze(
            dataframe,
            current,
            spec,
            config.country_code,
            context,
        ).cache()
        return Output(
            dataframe,
            metadata={
                **get_output_metadata(config),
                "row_count": dataframe.count(),
                "preview": get_table_preview(dataframe),
            },
        )

    report_schema = (
        f"{MEMBER_IDENTITY_SCHEMA}, duplicate_location_rows_id string, "
        "duplicate_location_rows_count int, duplicate_group_id_50m string, "
        "duplicate_group_count_50m int"
    )

    @multi_asset(
        name=spec.assets.dq_results,
        group_name=spec.group_name,
        ins={"bronze": AssetIn(key=_asset_key(spec.assets.bronze))},
        outs={
            spec.assets.dq_results: AssetOut(
                io_manager_key=ResourceKey.ADLS_SPARK_IO_MANAGER.value
            ),
            duplicates_name: AssetOut(
                io_manager_key=ResourceKey.ADLS_SPARK_SINGLE_FILE_IO_MANAGER.value
            ),
        },
    )
    def facility_dq(
        context: OpExecutionContext,
        bronze: sql.DataFrame,
        config: FileConfig,
        spark: PySparkResource,
    ):
        current = _load_current_facilities(
            spark.spark_session,
            spec,
            config.country_code,
            bronze.schema,
        )
        extra_outputs: dict = {}
        checked = run_facility_geolocation_checks(
            bronze,
            current,
            spec,
            config.country_code,
            context,
            extra_outputs,
        )
        location_members = extra_outputs.get("duplicate_location_members")
        proximity_members = extra_outputs.get("duplicate_50m_members")
        if location_members is None:
            location_members = spark.spark_session.createDataFrame([], report_schema)
        if proximity_members is None:
            proximity_members = spark.spark_session.createDataFrame([], report_schema)
        report = finalize_duplicates_report(
            attach_approval_status(
                combine_duplicate_members(location_members, proximity_members),
                checked,
                dq_id_column=spec.govt_id_column,
            ),
            spec.dq_config_table,
        )
        report_aliases = {
            "school_id_govt": spec.govt_id_column,
            "school_name": spec.name_column,
            "education_level_govt": spec.level_column,
        }
        for internal, external in report_aliases.items():
            if internal != external and internal in report.columns:
                report = report.withColumnRenamed(internal, external)
        checked = collapse_facility_dq_flags(checked).cache()
        yield Output(
            checked,
            output_name=spec.assets.dq_results,
            metadata={
                **get_output_metadata(config),
                "row_count": checked.count(),
                "preview": get_table_preview(checked),
            },
        )
        yield Output(
            report,
            output_name=duplicates_name,
            metadata={
                **get_output_metadata(config),
                "row_count": report.count(),
                "preview": get_table_preview(report),
            },
        )

    @multi_asset(
        name=f"{prefix}_human_readable",
        group_name=spec.group_name,
        ins={
            "dq_results": AssetIn(key=_asset_key(spec.assets.dq_results)),
        },
        outs={
            human_passed_name: AssetOut(
                io_manager_key=ResourceKey.ADLS_SPARK_SINGLE_FILE_IO_MANAGER.value
            ),
            human_failed_name: AssetOut(
                io_manager_key=ResourceKey.ADLS_SPARK_SINGLE_FILE_IO_MANAGER.value
            ),
        },
    )
    def facility_human_readable(
        dq_results: sql.DataFrame,
        config: FileConfig,
    ):
        rows = _human_readable_rows(
            dq_results,
            _uploaded_columns(config),
            spec,
        ).cache()
        passed = rows.filter(f.col("dq_has_critical_error") == 0).drop(
            "dq_has_critical_error", "failure_reason"
        )
        failed = rows.filter(f.col("dq_has_critical_error") == 1).drop(
            "dq_has_critical_error"
        )
        yield Output(passed, output_name=human_passed_name)
        yield Output(failed, output_name=human_failed_name)

    @asset(
        name=summary_name,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_JSON_IO_MANAGER.value,
        ins={
            "bronze": AssetIn(key=_asset_key(spec.assets.bronze)),
            "dq_results": AssetIn(key=_asset_key(spec.assets.dq_results)),
        },
    )
    async def facility_summary(
        context: OpExecutionContext,
        bronze: sql.DataFrame,
        dq_results: sql.DataFrame,
        config: FileConfig,
        spark: PySparkResource,
    ) -> Output[dict]:
        relevant, _ = dq_geolocation_extract_relevant_columns(
            dq_results,
            _uploaded_columns(config),
            dq_config_table=spec.dq_config_table,
            is_new_column="is_new_facility",
        )
        summary = build_dq_summary_statistics(
            spark.spark_session,
            relevant,
            bronze,
            is_new_column="is_new_facility",
            entity_plural="facilities",
            govt_id_column=spec.govt_id_column,
            dq_config_table=spec.dq_config_table,
        )
        value_maps = aggregate_value_maps_for_pdf(
            dq_results,
            uploaded_columns=_uploaded_columns(config),
        )
        if value_maps:
            summary["valueMaps"] = value_maps
        await send_email_dq_report_with_config(
            dq_results=summary,
            config=config,
            value_maps=value_maps or None,
            context=context,
        )
        return Output(summary, metadata=get_output_metadata(config))

    @asset(
        name=passed_name,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_SPARK_IO_MANAGER.value,
        ins={"dq_results": AssetIn(key=_asset_key(spec.assets.dq_results))},
    )
    def facility_passed(
        dq_results: sql.DataFrame,
        config: FileConfig,
    ) -> Output[sql.DataFrame]:
        passed, _ = split_facility_dq_rows(dq_results)
        return Output(passed, metadata=get_output_metadata(config))

    @asset(
        name=failed_name,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_SPARK_IO_MANAGER.value,
        ins={"dq_results": AssetIn(key=_asset_key(spec.assets.dq_results))},
    )
    def facility_failed(
        dq_results: sql.DataFrame,
        config: FileConfig,
    ) -> Output[sql.DataFrame]:
        _, failed = split_facility_dq_rows(dq_results)
        return Output(failed, metadata=get_output_metadata(config))

    @asset(
        name=error_name,
        group_name=spec.group_name,
        ins={"failed": AssetIn(key=_asset_key(failed_name))},
    )
    def facility_error_table(
        failed: sql.DataFrame,
        config: FileConfig,
    ) -> Output[None]:
        upload_id = config.filename_components.id
        full_table_name = construct_full_table_name(
            f"{spec.metastore_schema}_error_table",
            config.country_code,
        )
        create_schema(failed.sparkSession, f"{spec.metastore_schema}_error_table")
        if failed.sparkSession.catalog.tableExists(full_table_name):
            DeltaTable.forName(failed.sparkSession, full_table_name).delete(
                f.col("upload_id") == upload_id
            )
        if failed.isEmpty():
            return Output(None, metadata=get_output_metadata(config))
        (
            failed.withColumn("upload_id", f.lit(upload_id))
            .withColumn("created_at", f.current_timestamp())
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(full_table_name)
        )
        return Output(None, metadata=get_output_metadata(config))

    @asset(
        name=spec.assets.staging,
        group_name=spec.group_name,
        ins={"passed": AssetIn(key=_asset_key(passed_name))},
    )
    def facility_staging(
        context: OpExecutionContext,
        passed: sql.DataFrame,
        config: FileConfig,
        spark: PySparkResource,
        adls_file_client: ADLSFileClient,
    ) -> Output[None]:
        if config.metadata.get("dq_mode") == "uploaded" or passed.isEmpty():
            return Output(None, metadata=get_output_metadata(config))
        step = StagingStep(
            context,
            config,
            adls_file_client,
            spark.spark_session,
            StagingMode.UPDATE,
            match_key=spec.govt_id_column,
        )
        pending = step(passed)
        return Output(
            None,
            metadata=get_staging_change_type_metadata(pending, config),
        )

    @asset(name=delete_name, group_name=spec.group_name)
    def facility_delete_staging(
        context: OpExecutionContext,
        config: FileConfig,
        spark: PySparkResource,
        adls_file_client: ADLSFileClient,
    ) -> Output[None]:
        payload = adls_file_client.download_json(config.filepath)
        delete_ids = list(dict.fromkeys(payload.get("ids", [])))
        step = StagingStep(
            context,
            config,
            adls_file_client,
            spark.spark_session,
            StagingMode.DELETE,
        )
        pending = step(delete_ids, id_type=payload.get("id_type", spec.giga_id_column))
        return Output(
            None,
            metadata=get_staging_change_type_metadata(pending, config),
        )

    @asset(
        name=map_name,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_GENERIC_FILE_IO_MANAGER.value,
        ins={"dq_results": AssetIn(key=_asset_key(spec.assets.dq_results))},
    )
    def facility_map(
        context: OpExecutionContext,
        dq_results: sql.DataFrame,
        config: FileConfig,
    ) -> Output[str]:
        from src.utils.map_generator import generate_school_map_html

        frame = normalize_dq_results_map(dq_results)
        frame = build_dq_warning_columns(frame, spec.dq_config_table)
        passed = frame.filter(f.col("dq_has_critical_error") == 0).toPandas()
        failed = frame.filter(f.col("dq_has_critical_error") == 1).toPandas()
        html = generate_school_map_html(
            config.country_code,
            passed,
            failed,
            context,
            entity_label=f"{spec.facility_type.title()} Facilities",
            name_column=spec.name_column,
            giga_id_column=spec.giga_id_column,
            govt_id_column=spec.govt_id_column,
            level_column=spec.level_column,
        )
        return Output(html, metadata=get_output_metadata(config))

    @asset(
        name=kit_name,
        group_name=spec.group_name,
        io_manager_key=ResourceKey.ADLS_GENERIC_FILE_IO_MANAGER.value,
        deps=[_asset_key(map_name)],
    )
    def facility_dq_kit(
        context: OpExecutionContext,
        config: FileConfig,
        adls_file_client: ADLSFileClient,
    ) -> Output[bytes]:
        from src.utils.dq_kit_generator import generate_dq_kit_zip_bytes

        with get_db_context() as db:
            file_upload = db.scalar(
                select(FileUpload).where(
                    FileUpload.id == config.filename_components.id
                )
            )
            if file_upload is None:
                raise FileNotFoundError(
                    f"FileUpload `{config.filename_components.id}` was not found"
                )
        stem = Path(config.filepath).stem
        data, filename = generate_dq_kit_zip_bytes(
            country_code=config.country_code,
            upload_id=config.filename_components.id,
            dataset=config.dataset_type,
            original_filename=file_upload.original_filename,
            stem=stem,
            adls_client=adls_file_client,
            context=context,
            dataset_prefix=spec.paths.dq_directory,
            entity_plural=f"{spec.facility_type.title()} facilities",
            map_filename_prefix=f"{spec.facility_type}_facility_map",
            include_master_export=False,
        )
        return Output(
            data,
            metadata={
                **get_output_metadata(config),
                "zip_filename": filename,
                "zip_size_bytes": len(data),
            },
        )

    return [
        facility_raw,
        facility_metadata,
        facility_bronze,
        facility_dq,
        facility_human_readable,
        facility_summary,
        facility_passed,
        facility_failed,
        facility_error_table,
        facility_staging,
        facility_delete_staging,
        facility_map,
        facility_dq_kit,
    ]
