from datetime import UTC, datetime, timedelta
from enum import Enum
from string import punctuation

from croniter import croniter
from models.qos_apis import SchoolConnectivity, SchoolList
from sqlalchemy.orm import joinedload

from dagster import RunConfig, RunRequest, SkipReason, sensor
from src.constants import DataTier, constants
from src.jobs.qos import (
    qos_school_connectivity__automated_data_checks_job,
    qos_school_list__automated_data_checks_job,
)
from src.schemas.qos import SchoolConnectivityConfig, SchoolListConfig
from src.settings import settings
from src.utils.db.primary import get_db_context
from src.utils.op_config import OpDestinationMapping, generate_run_ops


DOMAIN = "connectivity"
DATAHUB_DOMAIN = "School"
DATASET_TYPE = "geolocation"

DOMAIN_DATASET_TYPE = f"{DOMAIN}-{DATASET_TYPE}"
METASTORE_SCHEMA = f"{DOMAIN}_{DATASET_TYPE}"


@sensor(
    job=qos_school_list__automated_data_checks_job,
    minimum_interval_seconds=int(timedelta(days=1).total_seconds())
    if settings.IN_PRODUCTION
    else 60,
)

def school_connectivity_new_connected_schools_sensor(context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,):
    count = 0
    source_directory = constants.connectivity_updates_folder

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
            properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
            metadata = properties.metadata