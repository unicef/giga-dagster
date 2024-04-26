from pathlib import Path

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.constants import DataTier, constants
from src.jobs.school_master import (
    school_master_coverage__automated_data_checks_job,
    school_master_coverage__failed_manual_checks_job,
    school_master_coverage__successful_manual_checks_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.filename import deconstruct_school_master_filename_components
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DATASET_TYPE = "coverage"
DOMAIN = "school"
SCHOOL_DATASET_TYPE = f"{DOMAIN}-{DATASET_TYPE}"


@sensor(
    job=school_master_coverage__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__raw_file_uploads_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = f"{constants.UPLOAD_PATH_PREFIX}/{SCHOOL_DATASET_TYPE}"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        try:
            filename_components = deconstruct_school_master_filename_components(
                adls_filepath
            )
        except Exception as e:
            context.log.error(f"Failed to deconstruct filename: {adls_filepath}: {e}")
            continue
        else:
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
                "coverage_staging": OpDestinationMapping(
                    source_filepath=f"{constants.bronze_folder}/{SCHOOL_DATASET_TYPE}/{country_code}/{stem}.csv",
                    destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_coverage_staging.db/{country_code.lower()}",
                    metastore_schema=metastore_schema,
                    tier=DataTier.STAGING,
                ),
            }

            run_ops = generate_run_ops(
                ops_destination_mapping,
                dataset_type=DATASET_TYPE,
                metadata=metadata,
                file_size_bytes=size,
                domain=DOMAIN,
                dq_target_filepath=f"{constants.UPLOAD_PATH_PREFIX}/{SCHOOL_DATASET_TYPE}/{country_code}/{stem}{path.suffix}",
                country_code=country_code,
            )

            context.log.info(f"FILE: {path}")
            yield RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            )
            count += 1

    if count == 0:
        yield SkipReason(f"No uploads detected in {source_directory}")


@sensor(
    job=school_master_coverage__successful_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__successful_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = (
        f"{constants.staging_folder}/approved-row-ids/{SCHOOL_DATASET_TYPE}"
    )

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        try:
            filename_components = deconstruct_school_master_filename_components(
                adls_filepath
            )
        except Exception as e:
            context.log.error(f"Failed to deconstruct filename: {adls_filepath}: {e}")
            continue
        else:
            country_code = filename_components.country_code

            ops_destination_mapping = {
                "silver": OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_coverage_silver.db/{country_code.lower()}",
                    metastore_schema="school_coverage",
                    tier=DataTier.SILVER,
                ),
                "master": OpDestinationMapping(
                    source_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_coverage_silver.db/{country_code.lower()}",
                    destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_master.db/{country_code.lower()}",
                    metastore_schema="school_master",
                    tier=DataTier.GOLD,
                ),
                "reference": OpDestinationMapping(
                    source_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_coverage_silver.db/{country_code.lower()}",
                    destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_reference.db/{country_code.lower()}",
                    metastore_schema="school_reference",
                    tier=DataTier.GOLD,
                ),
            }

            run_ops = generate_run_ops(
                ops_destination_mapping,
                dataset_type=DATASET_TYPE,
                metadata={},
                file_size_bytes=0,
                domain=DOMAIN,
                country_code=country_code,
            )

            context.log.info(f"FILE: {path}")
            yield RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            )
            count += 1

    if count == 0:
        yield SkipReason(f"No files detected in {source_directory}")


@sensor(
    job=school_master_coverage__failed_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__failed_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = (
        f"{constants.staging_folder}/approved-row-ids/{SCHOOL_DATASET_TYPE}"
    )

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        try:
            filename_components = deconstruct_school_master_filename_components(
                adls_filepath
            )
        except Exception as e:
            context.log.error(f"Failed to deconstruct filename: {adls_filepath}: {e}")
            continue
        else:
            country_code = filename_components.country_code
            metastore_schema = "school_coverage"

            ops_destination_mapping = {
                "manual_review_failed_rows": OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=str(path),
                    metastore_schema=metastore_schema,
                    tier=DataTier.RAW,
                ),
            }

            run_ops = generate_run_ops(
                ops_destination_mapping,
                dataset_type=DATASET_TYPE,
                metadata={},
                file_size_bytes=0,
                domain=DOMAIN,
                country_code=country_code,
            )

            context.log.info(f"FILE: {path}")
            yield RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            )
            count += 1

    if count == 0:
        yield SkipReason(f"No files detected in {source_directory}")
