from dagster import Config, RunConfig, RunRequest, sensor
from src.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_failed_manual_checks_job,
    school_master__run_successful_manual_checks_job,
)
from src.resources.adls_file_client import ADLSFileClient


class FileConfig(Config):
    filepath: str
    dataset_type: str


def get_dataset_type(filepath: str) -> str:
    if "geolocation" in filepath:
        return "school-geolocation-data"
    elif "coverage" in filepath:
        return "school-coverage-data"


@sensor(job=school_master__run_automated_data_checks_job, minimum_interval_seconds=60)
def school_master__raw_file_uploads_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths("adls-testing-raw")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "raw": FileConfig(filepath=filepath, dataset_type=dataset_type),
                        "bronze": FileConfig(
                            filepath=filepath, dataset_type=dataset_type
                        ),
                        "dq_passed_rows": FileConfig(
                            filepath=filepath, dataset_type=dataset_type
                        ),
                        "dq_failed_rows": FileConfig(
                            filepath=filepath, dataset_type=dataset_type
                        ),
                    }
                ),
            )


@sensor(
    job=school_master__run_successful_manual_checks_job, minimum_interval_seconds=60
)
def school_master__successful_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths("staging/approved")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_passed_rows": FileConfig(
                            filepath=filepath, dataset_type=dataset_type
                        ),
                        "silver": FileConfig(
                            filepath=filepath, dataset_type=dataset_type
                        ),
                        "gold": FileConfig(
                            filepath=filepath, dataset_type=dataset_type
                        ),
                    }
                ),
            )


@sensor(job=school_master__run_failed_manual_checks_job, minimum_interval_seconds=60)
def school_master__failed_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths("archive/manual-review-rejected")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_failed_rows": FileConfig(
                            filepath=filepath, dataset_type=dataset_type
                        ),
                    }
                ),
            )
