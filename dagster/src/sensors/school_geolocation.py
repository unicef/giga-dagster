from dagster import RunConfig, RunRequest, SensorEvaluationContext, sensor
from src.constants import constants
from src.jobs.school_master import (
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__failed_manual_checks_job,
    school_master_geolocation__successful_manual_checks_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient

from .base import (
    AssetOpDestinationMapping,
    MultiAssetOpDestinationMapping,
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
    for file_data in adls_file_client.list_paths_generator(
        f"{constants.raw_folder}/{SCHOOL_DATASET_TYPE}", recursive=False
    ):
        if file_data.is_directory:
            continue

        filepath = file_data.name
        properties = adls_file_client.get_file_metadata(filepath=filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_geolocation"

        ops_destination_mapping = {
            "geolocation_raw": AssetOpDestinationMapping(
                path=f"{constants.raw_folder}/{SCHOOL_DATASET_TYPE}",
                metastore_schema=metastore_schema,
            ),
            "geolocation_bronze": AssetOpDestinationMapping(
                path=f"{constants.bronze_folder}/{SCHOOL_DATASET_TYPE}",
                metastore_schema=metastore_schema,
            ),
            "geolocation_data_quality_results": MultiAssetOpDestinationMapping(
                path={
                    "geolocation_dq_results": f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-overall",
                    "geolocation_dq_summary_statistics": f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-summary",
                },
                metastore_schema={
                    "geolocation_dq_results": metastore_schema,
                    "geolocation_dq_summary_statistics": metastore_schema,
                },
            ),
            "geolocation_dq_passed_rows": AssetOpDestinationMapping(
                path=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-passed-rows",
                metastore_schema=metastore_schema,
            ),
            "geolocation_dq_failed_rows": AssetOpDestinationMapping(
                path=f"{constants.dq_results_folder}/{SCHOOL_DATASET_TYPE}/dq-failed-rows",
                metastore_schema=metastore_schema,
            ),
            # "geolocation_staging": AssetOpDestinationMapping(
            #     path=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}", metastore_schema=metastore_schema
            # ),
        }

        run_ops = generate_run_ops(
            context,
            ops_destination_mapping,
            filepath=filepath,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {filepath}")
        yield RunRequest(
            run_key=filepath,
            run_config=RunConfig(ops=run_ops),
        )


@sensor(
    job=school_master_geolocation__successful_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__successful_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    for file_data in adls_file_client.list_paths_generator(
        f"{constants.dq_passed_folder}/{SCHOOL_DATASET_TYPE}", recursive=False
    ):
        if file_data.is_directory:
            continue

        filepath = file_data.name
        properties = adls_file_client.get_file_metadata(filepath=filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_geolocation"

        ops_destination_mapping = {
            "manual_review_passed_rows": AssetOpDestinationMapping(
                path=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/approved-rows",
                metastore_schema=metastore_schema,
            ),
            "geolocation_silver": AssetOpDestinationMapping(
                path=f"{constants.silver_folder}/{SCHOOL_DATASET_TYPE}",
                metastore_schema=metastore_schema,
            ),
            "geolocation_gold": MultiAssetOpDestinationMapping(
                path={
                    "master": f"{constants.gold_folder}/school-master",
                    "reference": f"{constants.gold_folder}/school-reference",
                },
                metastore_schema={
                    "master": "school_master",
                    "reference": "school_reference",
                },
            ),
        }

        run_ops = generate_run_ops(
            context,
            ops_destination_mapping,
            filepath=filepath,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {filepath}")
        yield RunRequest(run_key=f"{filepath}", run_config=RunConfig(ops=run_ops))


@sensor(
    job=school_master_geolocation__failed_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__failed_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    for file_data in adls_file_client.list_paths_generator(
        f"{constants.archive_manual_review_rejected_folder}", recursive=False
    ):
        if file_data.is_directory:
            continue

        filepath = file_data.name
        properties = adls_file_client.get_file_metadata(filepath=filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_geolocation"

        ops_destination_mapping = {
            "manual_review_failed_rows": AssetOpDestinationMapping(
                path=f"{constants.staging_folder}/{SCHOOL_DATASET_TYPE}/rejected-rows",
                metastore_schema=metastore_schema,
            ),
        }

        run_ops = generate_run_ops(
            context,
            ops_destination_mapping,
            filepath=filepath,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {filepath}")
        yield RunRequest(run_key=f"{filepath}", run_config=RunConfig(ops=run_ops))
