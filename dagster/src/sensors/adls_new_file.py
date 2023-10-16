from datetime import datetime, timedelta

from dagster import RunRequest, SensorEvaluationContext, sensor
from src.io_managers.common import get_fs_client
from src.jobs.log_added_file import log_file_job

ADLS_SENSE_DIRECTORY = "sensor-test"


@sensor(
    job=log_file_job,
    minimum_interval_seconds=int(
        timedelta(minutes=5).total_seconds(),
    ),
)
def adls_new_file(context: SensorEvaluationContext):
    last_creation_time = float(context.cursor) if context.cursor else 0
    max_creation_time = last_creation_time

    fs_client = get_fs_client()
    for path in fs_client.get_paths(path=ADLS_SENSE_DIRECTORY, recursive=False):
        path_creation_time: datetime = path["creation_time"]
        path_time = path_creation_time.timestamp()
        if path_time <= last_creation_time:
            continue

        yield RunRequest(
            run_key=path["name"],
            run_config={
                "ops": {
                    "read_filename": {
                        "config": {
                            "filename": path["name"],
                        },
                    },
                },
            },
        )
        max_creation_time = max(max_creation_time, path_time)

    context.update_cursor(str(max_creation_time))
