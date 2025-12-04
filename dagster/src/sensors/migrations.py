from dagster import RunRequest, sensor
from src.constants import constants
from src.jobs.migrations import migrate__schema
from src.settings import settings
from src.utils.adls import ADLSFileClient


@sensor(
    job=migrate__schema,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def migrations__schema_sensor():
    adls = ADLSFileClient()
    paths = adls.list_paths(constants.raw_schema_folder)

    for path in paths:
        if path.is_directory:
            continue

        filepath = path["name"]
        properties = adls.get_file_metadata(filepath=filepath)
        last_modified = properties.last_modified.strftime("%Y%m%d-%H%M%S")
        yield RunRequest(run_key=f"{filepath}:{last_modified}")
