from dagster import Config, RunConfig, RunRequest, sensor
from src._utils.adls import ADLSFileClient
from src.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_failed_manual_checks_job,
    school_master__run_successful_manual_checks_job,
)


class FileConfig(Config):
    filepath: str
    dataset_type: str


def get_dataset_type(filepath: str) -> str | None:
    if "geolocation" in filepath:
        return "school-geolocation-data"
    elif "coverage" in filepath:
        return "school-coverage-data"
    else:
        return None


@sensor(job=school_master__run_automated_data_checks_job, minimum_interval_seconds=30)
def school_master__raw_file_uploads_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths("adls-testing-raw")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        elif file_data["name"].endswith("test.csv"):
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            file_config = FileConfig(filepath=filepath, dataset_type=dataset_type)

            print(f"FILE: {filepath}")

            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "raw": file_config,
                        "bronze": file_config,
                        "data_quality_results": file_config,
                        "dq_passed_rows": file_config,
                        # "dq_failed_rows": file_config
                        # "dq_failed_rows": file_config,
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
            if dataset_type is None:
                continue

            file_config = FileConfig(filepath=filepath, dataset_type=dataset_type)

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
            if dataset_type is None:
                continue

            file_config = FileConfig(filepath=filepath, dataset_type=dataset_type)

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_failed_rows": file_config,
                    }
                ),
            )
