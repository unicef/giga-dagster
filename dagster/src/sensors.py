from dagster import Config, RunConfig, RunRequest, sensor
from src.constants import constants
from src.jobs import (
    school_master__automated_data_checks_job,
    school_master__convert_file_to_deltatable_job,
    school_master__failed_manual_checks_job,
    school_master__successful_manual_checks_job,
)
from src.utils.adls import ADLSFileClient


class FileConfig(Config):
    filepath: str
    dataset_type: str
    metadata: dict
    file_size_bytes: int


def get_dataset_type(filepath: str) -> str | None:
    if "geolocation" in filepath:
        return "school-geolocation-data"
    elif "coverage" in filepath:
        return "school-coverage-data"
    else:
        return None


@sensor(job=school_master__automated_data_checks_job, minimum_interval_seconds=30)
def school_master__raw_file_uploads_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.raw_folder}")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]
            file_config = FileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
            )

            print(f"FILE: {filepath}")

            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "raw": file_config,
                        "bronze": file_config,
                        "data_quality_results": file_config,
                        "dq_passed_rows": file_config,
                        "dq_failed_rows": file_config,
                    }
                ),
            )


@sensor(job=school_master__successful_manual_checks_job, minimum_interval_seconds=60)
def school_master__successful_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.staging_approved_folder}")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]
            file_config = FileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_passed_rows": file_config,
                        "silver": file_config,
                        "gold": file_config,
                    }
                ),
            )


@sensor(job=school_master__failed_manual_checks_job, minimum_interval_seconds=60)
def school_master__failed_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.archive_manual_review_rejected_folder}")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]
            file_config = FileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
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


@sensor(job=school_master__convert_file_to_deltatable_job, minimum_interval_seconds=30)
def school_master__file_to_deltatable_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.fake_gold_folder}")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]
            file_config = FileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "gold_delta_table_from_csv": file_config,
                    }
                ),
            )
