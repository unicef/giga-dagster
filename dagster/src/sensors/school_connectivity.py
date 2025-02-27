from pathlib import Path

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.constants import DataTier, constants
from src.jobs.school_connectivity import (
    school_connectivity__update_schools_realtime_status_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DATASET_TYPE = "geolocation"
DOMAIN = "school"
METASTORE_SCHEMA = "school_master"


@sensor(
    job=school_connectivity__update_schools_realtime_status_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_connectivity_update_schools_connectivity_sensor(
    context: SensorEvaluationContext, adls_file_client: ADLSFileClient
):
    count = 0
    source_directory = constants.connectivity_updates_folder

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)

        country_code, *_ = path.stem.split("_")
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size

        ops_destination_mapping = {
            "school_connectivity_realtime_master": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_master.db/{country_code.upper()}",
                metastore_schema=METASTORE_SCHEMA,
                tier=DataTier.GOLD,
            )
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
            domain=DOMAIN,
            dq_target_filepath=None,
            country_code=country_code,
        )

        context.log.info(f"FILE: {path}")
        yield RunRequest(
            run_key=str(path),
            run_config=RunConfig(ops=run_ops),
            tags={"country": country_code},
        )
        count += 1

    if count == 0:
        yield SkipReason(f"No uploads detected in {source_directory}")
