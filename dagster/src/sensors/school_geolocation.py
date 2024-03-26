from pathlib import Path

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.constants import constants
from src.jobs.school_master import (
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__failed_manual_checks_job,
    school_master_geolocation__successful_manual_checks_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient

from .base import (
    OpDestinationMapping,
    generate_run_ops,
)

DATASET_TYPE = "geolocation"
SCHOOL_DATASET_TYPE = f"school-{DATASET_TYPE}"


@sensor(
    job=school_master_geolocation__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__raw_file_uploads_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = f"{constants.raw_folder}/{SCHOOL_DATASET_TYPE}"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=False
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_geolocation"

        ops_destination_mapping = {
            "geolocation_raw": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
            ),
            "geolocation_bronze": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.bronze_folder}/{SCHOOL_DATASET_TYPE}/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "geolocation_data_quality_results": OpDestinationMapping(
                source_filepath=f"{constants.bronze_folder}/{SCHOOL_DATASET_TYPE}/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "geolocation_data_quality_results_summary": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-summary/{stem}.json",
                metastore_schema=metastore_schema,
            ),
            "geolocation_dq_passed_rows": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-passed-rows/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "geolocation_dq_failed_rows": OpDestinationMapping(
                source_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall/{stem}.csv",
                destination_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-failed-rows/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            # "geolocation_staging": OpDestinationMapping(
            #     source_filepath=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-passed-rows/{stem}.csv",
            #     destination_filepath=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/{stem}",
            #     metastore_schema=metastore_schema,
            # ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
            dq_target_filepath=f"{constants.bronze_folder}/{SCHOOL_DATASET_TYPE}/{stem}.csv",
        )

        context.log.info(f"FILE: {path}")
        yield RunRequest(run_key=str(path), run_config=RunConfig(ops=run_ops))
        count += 1

    if count == 0:
        yield SkipReason(f"No uploads detected in {source_directory}")


@sensor(
    job=school_master_geolocation__successful_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__successful_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = f"{constants.dq_passed_folder}/{SCHOOL_DATASET_TYPE}"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=False
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_geolocation"

        ops_destination_mapping = {
            "manual_review_passed_rows": OpDestinationMapping(
                source_filepath=str(path),
                # TODO: Finalize format
                destination_filepath=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/approved-rows/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "silver": OpDestinationMapping(
                source_filepath=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/approved-rows/{stem}.csv",
                destination_filepath=f"{constants.silver_folder}/{SCHOOL_DATASET_TYPE}/{stem}",
                metastore_schema=metastore_schema,
            ),
            "gold_master": OpDestinationMapping(
                source_filepath=f"{constants.silver_folder}/{SCHOOL_DATASET_TYPE}/{stem}",
                destination_filepath=f"{constants.gold_folder}/school-master/{stem}",
                metastore_schema="school_master",
            ),
            "gold_reference": OpDestinationMapping(
                source_filepath=f"{constants.silver_folder}/{SCHOOL_DATASET_TYPE}/{stem}",
                destination_filepath=f"{constants.gold_folder}/school-reference/{stem}",
                metastore_schema="school_reference",
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {path}")
        yield RunRequest(run_key=str(path), run_config=RunConfig(ops=run_ops))
        count += 1

    if count == 0:
        yield SkipReason(f"No files detected in {source_directory}")


@sensor(
    job=school_master_geolocation__failed_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__failed_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = (
        f"{constants.archive_manual_review_rejected_folder}/{SCHOOL_DATASET_TYPE}"
    )

    for file_data in adls_file_client.list_paths_generator(
        source_directory,
        recursive=False,
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_geolocation"

        ops_destination_mapping = {
            "manual_review_failed_rows": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/rejected-rows/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {path}")
        yield RunRequest(run_key=str(path), run_config=RunConfig(ops=run_ops))
        count += 1

    if count == 0:
        yield SkipReason(f"No files detected in {source_directory}")
