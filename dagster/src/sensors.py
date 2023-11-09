import os

from dagster import Config, RunConfig, RunRequest, sensor
from src._utils.io_managers import ADLSFileClient
from src.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_manual_checks_and_transforms_job,
)


class FileConfig(Config):
    filepath: str


@sensor(job=school_master__run_automated_data_checks_job, minimum_interval_seconds=60)
def school_master__run_automated_data_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths("adls-testing-raw")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "raw": FileConfig(filepath=filepath),
                        "bronze": FileConfig(filepath=filepath),
                        "dq_passed_rows": FileConfig(filepath=filepath),
                        "dq_failed_rows": FileConfig(filepath=filepath),
                    }
                ),
            )


@sensor(job=school_master__run_manual_checks_and_transforms_job)
def school_master__run_manual_checks_and_transforms_sensor():
    folders = [
        "school-geolocation-data",
        "school-coverage-data",
        "infrastructure-data",
        "geospatial-data",
    ]

    for folder in folders:
        for filepath in os.listdir(f"/staging/pending-review/{folder}"):
            filepath = os.path.join("/staging/pending-review", filepath)
            if os.path.isfile(filepath):
                yield RunRequest(
                    run_key=filepath,
                )
