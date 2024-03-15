from dagster import RunConfig, RunRequest, sensor
from src.constants import constants
from src.jobs.school_master import (
    school_master_coverage__automated_data_checks_job,
    school_master_coverage__failed_manual_checks_job,
    school_master_coverage__successful_manual_checks_job,
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__failed_manual_checks_job,
    school_master_geolocation__successful_manual_checks_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient

from .base import FileConfig, get_dataset_type


@sensor(
    job=school_master_geolocation__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__raw_file_uploads_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.raw_folder}/school-geolocation-data")

    ops_list = [
        "geolocation_raw",
        "geolocation_bronze",
        "geolocation_data_quality_results",
        "geolocation_dq_passed_rows",
        "geolocation_dq_failed_rows",
        # 'geolocation_staging',
    ]

    for file_data in file_list:
        if file_data.is_directory:
            continue

        filepath = file_data.name

        properties = adls.get_file_metadata(filepath=filepath)
        metadata = properties.metadata
        size = properties.size

        file_config = FileConfig(
            filepath=filepath,
            dataset_type="geolocation",
            metadata=metadata,
            file_size_bytes=size,
            metastore_schema="school_geolocation",
        )

        print(f"FILE: {filepath}")

        yield RunRequest(
            run_key=f"{filepath}",
            run_config=RunConfig(
                ops=dict(zip(ops_list, [file_config] * len(ops_list), strict=True))
            ),
        )


@sensor(
    job=school_master_coverage__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__raw_file_uploads_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.raw_folder}/school-coverage-data")

    ops_list = [
        "coverage_raw",
        "coverage_data_quality_results",
        "coverage_dq_passed_rows",
        "coverage_dq_failed_rows",
        "coverage_bronze",
        # 'coverage_staging',
    ]

    for file_data in file_list:
        if file_data.is_directory:
            continue

        filepath = file_data.name

        properties = adls.get_file_metadata(filepath=filepath)
        metadata = properties.metadata
        size = properties.size

        file_config = FileConfig(
            filepath=filepath,
            dataset_type="coverage",
            metadata=metadata,
            file_size_bytes=size,
            metastore_schema="school_coverage",
        )

        print(f"FILE: {filepath}")

        yield RunRequest(
            run_key=f"{filepath}",
            run_config=RunConfig(
                ops=dict(zip(ops_list, [file_config] * len(ops_list), strict=True))
            ),
        )


@sensor(
    job=school_master_geolocation__successful_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__successful_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.dq_passed_folder}/school-geolocation-data")

    ops_list = [
        "manual_review_passed_rows",
        "geolocation_bronze",
        "geolocation_gold",
    ]

    for file_data in file_list:
        if file_data.is_directory:
            continue

        filepath = file_data.name
        dataset_type = get_dataset_type(filepath)
        if dataset_type is None:
            continue

        properties = adls.get_file_metadata(filepath=filepath)
        metadata = properties.metadata
        size = properties.size

        file_config = FileConfig(
            filepath=filepath,
            dataset_type=dataset_type,
            metadata=metadata,
            file_size_bytes=size,
            metastore_schema="school_master",
        )

        print(f"FILE: {filepath}")

        yield RunRequest(
            run_key=f"{filepath}",
            run_config=RunConfig(
                ops=dict(zip(ops_list, [file_config] * len(ops_list), strict=True))
            ),
        )


@sensor(
    job=school_master_coverage__successful_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__successful_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.dq_passed_folder}/school-coverage-data")

    ops_list = [
        "manual_review_passed_rows",
        "coverage_bronze",
        "coverage_gold",
    ]

    for file_data in file_list:
        if file_data.is_directory:
            continue

        filepath = file_data.name
        dataset_type = get_dataset_type(filepath)
        if dataset_type is None:
            continue

        properties = adls.get_file_metadata(filepath=filepath)
        metadata = properties.metadata
        size = properties.size

        file_config = FileConfig(
            filepath=filepath,
            dataset_type=dataset_type,
            metadata=metadata,
            file_size_bytes=size,
            metastore_schema="school_master",
        )

        print(f"FILE: {filepath}")

        yield RunRequest(
            run_key=f"{filepath}",
            run_config=RunConfig(
                ops=dict(zip(ops_list, [file_config] * len(ops_list), strict=True))
            ),
        )


@sensor(
    job=school_master_geolocation__failed_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__failed_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.archive_manual_review_rejected_folder}")

    for file_data in file_list:
        if file_data.is_directory:
            continue
        else:
            filepath = file_data.name
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties.metadata
            size = properties.size
            file_config = FileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema="school_geolocation",
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_failed_rows": file_config,
                    }
                ),
            )


@sensor(
    job=school_master_coverage__failed_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__failed_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.archive_manual_review_rejected_folder}")

    for file_data in file_list:
        if file_data.is_directory:
            continue
        else:
            filepath = file_data.name
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties.metadata
            size = properties.size
            file_config = FileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema="school_coverage",
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_failed_rows": file_config,
                    }
                ),
            )
