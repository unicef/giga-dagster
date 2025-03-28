from pathlib import Path
from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.constants import DataTier, constants
from src.jobs.qos import (
    qos_availability_create_silver_job
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.op_config import OpDestinationMapping, generate_run_ops
from src.utils.filename import (
    deconstruct_adhoc_filename_components,
)

DATASET_TYPE = "availability"
DOMAIN = "qos"
DOMAIN_DATASET_TYPE = f"{DOMAIN}-{DATASET_TYPE}"
METASTORE_SCHEMA = f"{DOMAIN}_{DATASET_TYPE}"

@sensor(
    job=qos_availability_create_silver_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def qos_availability__raw_file_uploads_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = f"{constants.raw_folder}/{DOMAIN_DATASET_TYPE}"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        filename_components = deconstruct_adhoc_filename_components(file_data.name)
        country_code = filename_components.country_code

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size

        ops_destination_mapping = {
            "qos_availability_raw": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=METASTORE_SCHEMA,
                tier=DataTier.RAW,
            ),
            "qos_availability_bronze": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.bronze_folder}/qos.db/availability",
                metastore_schema="qos_bronze",
                tier=DataTier.BRONZE,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type=DATASET_TYPE,
            metadata=metadata,
            file_size_bytes=size,
            domain=DOMAIN,
            country_code=country_code
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
