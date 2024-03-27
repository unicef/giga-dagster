from pathlib import Path

from dagster import RunConfig, RunRequest, SensorEvaluationContext, sensor
from src.constants import DataTier, constants
from src.jobs.school_master import (
    school_master_coverage__automated_data_checks_job,
    school_master_coverage__failed_manual_checks_job,
    school_master_coverage__successful_manual_checks_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.filename import deconstruct_filename_components
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DATASET_TYPE = "coverage"
SCHOOL_DATASET_TYPE = f"school-{DATASET_TYPE}"


@sensor(
    job=school_master_coverage__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__raw_file_uploads_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    for file_data in adls_file_client.list_paths_generator(
        f"{constants.raw_folder}/{SCHOOL_DATASET_TYPE}", recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        filename_components = deconstruct_filename_components(adls_filepath)
        country_code = filename_components.country_code
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_coverage"

        ops_destination_mapping = {
            "coverage_raw": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
            "coverage_data_quality_results": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "coverage_data_quality_results_summary": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-summary/{country_code}/{stem}.json",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "coverage_dq_passed_rows": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-passed-rows/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "coverage_dq_failed_rows": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-failed-rows/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "coverage_bronze": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-passed-rows/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.bronze_folder}/{SCHOOL_DATASET_TYPE}/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.BRONZE,
            ),
            # "coverage_staging": OpDestinationMapping(
            #     source_filepath=f"{constants.bronze_folder}/{SCHOOL_DATASET_TYPE}/{country_code}/{stem}.csv",
            #     destination_filepath=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/{country_code}/{stem}",
            #     metastore_schema=metastore_schema,
            #     tier=DataTier.STAGING,
            # ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {path}")
        yield RunRequest(
            run_key=str(path),
            run_config=RunConfig(ops=run_ops),
            tags={"country_code": country_code},
        )


@sensor(
    job=school_master_coverage__successful_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__successful_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    for file_data in adls_file_client.list_paths_generator(
        f"{constants.dq_passed_folder}/{SCHOOL_DATASET_TYPE}", recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        filename_components = deconstruct_filename_components(adls_filepath)
        country_code = filename_components.country_code
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_coverage"

        ops_destination_mapping = {
            "manual_review_passed_rows": OpDestinationMapping(
                source_filepath=str(path),
                # TODO: Finalize format
                destination_filepath=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/approved-rows/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
            "silver": OpDestinationMapping(
                source_filepath=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/approved-rows/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.silver_folder}/{SCHOOL_DATASET_TYPE}/{country_code}/{stem}",
                metastore_schema=metastore_schema,
                tier=DataTier.SILVER,
            ),
            "gold_master": OpDestinationMapping(
                source_filepath=f"{constants.silver_folder}/{SCHOOL_DATASET_TYPE}/{country_code}/{stem}",
                destination_filepath=f"{constants.gold_folder}/school-master/{stem}",
                metastore_schema="school_master",
                tier=DataTier.GOLD,
            ),
            "gold_reference": OpDestinationMapping(
                source_filepath=f"{constants.silver_folder}/{SCHOOL_DATASET_TYPE}/{country_code}/{stem}",
                destination_filepath=f"{constants.gold_folder}/school-reference/{stem}",
                metastore_schema="school_reference",
                tier=DataTier.GOLD,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {path}")
        yield RunRequest(
            run_key=str(path),
            run_config=RunConfig(ops=run_ops),
            tags={"country_code": country_code},
        )


@sensor(
    job=school_master_coverage__failed_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__failed_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    for file_data in adls_file_client.list_paths(
        f"{constants.archive_manual_review_rejected_folder}/{SCHOOL_DATASET_TYPE}",
        recursive=True,
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        filename_components = deconstruct_filename_components(adls_filepath)
        country_code = filename_components.country_code
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_coverage"

        ops_destination_mapping = {
            "manual_review_failed_rows": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/rejected-rows/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {path}")
        yield RunRequest(
            run_key=str(path),
            run_config=RunConfig(ops=run_ops),
            tags={"country_code": country_code},
        )
