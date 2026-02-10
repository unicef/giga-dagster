from pathlib import Path

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.constants import DataTier, constants
from src.jobs.unstructured import (
    generalized_unstructured__emit_metadata_to_datahub_job,
    unstructured__emit_metadata_to_datahub_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.filename import deconstruct_unstructured_filename_components
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DATASET_TYPE = "unstructured"
DOMAIN = ""
DOMAIN_DATASET_TYPE = DATASET_TYPE
METASTORE_SCHEMA = f"{DOMAIN}_{DATASET_TYPE}"


@sensor(
    job=unstructured__emit_metadata_to_datahub_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def unstructured__emit_metadata_to_datahub_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = f"{constants.UPLOAD_PATH_PREFIX}/{DOMAIN_DATASET_TYPE}"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        try:
            filename_components = deconstruct_unstructured_filename_components(
                adls_filepath
            )
        except Exception as e:
            context.log.error(f"Failed to deconstruct filename: {adls_filepath}: {e}")
            continue
        else:
            country_code = filename_components.country_code
            metadata = adls_file_client.fetch_metadata_for_blob(adls_filepath) or {}
            properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
            size = properties.size

            ops_destination_mapping = {
                "unstructured_raw": OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=str(path),
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.RAW,
                ),
            }

            run_ops = generate_run_ops(
                ops_destination_mapping,
                dataset_type=DATASET_TYPE,
                metadata=metadata,
                file_size_bytes=size,
                domain=DOMAIN,
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


@sensor(
    job=generalized_unstructured__emit_metadata_to_datahub_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def generalized_unstructured__emit_metadata_to_datahub_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = "legacy_data"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)

        metadata = adls_file_client.fetch_metadata_for_blob(adls_filepath) or {}
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        size = properties.size
        last_modified = properties.last_modified.strftime("%Y%m%d-%H%M%S")

        ops_destination_mapping = {
            "generalized_unstructured_raw": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema="",
                tier=DataTier.RAW,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="",
            metadata=metadata,
            file_size_bytes=size,
            domain="",
            country_code="",
        )

        context.log.info(f"FILE: {path}")
        yield RunRequest(
            run_key=f"{path}:{last_modified}",
            run_config=RunConfig(ops=run_ops),
        )
        count += 1

    if count == 0:
        yield SkipReason(f"No uploads detected in {source_directory}")
