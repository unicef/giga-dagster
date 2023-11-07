import os

from dagster import RunRequest, sensor
from dagster.src.jobs import (
    school_master__run_automated_data_checks_job,
    school_master__run_manual_checks_and_transforms_job,
)


@sensor(job=school_master__run_automated_data_checks_job)
def school_master__run_automated_data_checks_sensor():
    folders = [
        "school-geolocation-data",
        "school-coverage-data",
        "infrastructure-data",
        "geospatial-data",
    ]

    for folder in folders:
        for filename in os.listdir(f"/raw/{folder}"):
            filepath = os.path.join("/raw/", filename)
            if os.path.isfile(filepath):
                yield RunRequest(
                    run_key=filename,
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
        for filename in os.listdir(f"/staging/pending-review/{folder}"):
            filepath = os.path.join("/staging/pending-review", filename)
            if os.path.isfile(filepath):
                yield RunRequest(
                    run_key=filename,
                )
