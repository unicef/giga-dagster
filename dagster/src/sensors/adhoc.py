from dagster import RunConfig, RunRequest, sensor
from src.constants import constants
from src.jobs.adhoc import (
    qos__convert_csv_to_deltatable_job,
    school_master__convert_gold_csv_to_deltatable_job,
    school_reference__convert_gold_csv_to_deltatable_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient

from .base import FileConfig, get_dataset_type


@sensor(
    job=school_master__convert_gold_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master__gold_csv_to_deltatable_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.gold_folder}/master")
    run_requests = []

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]
            file_config = FileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema=f"gold_master_{dataset_type.replace('-', '_')}",
            )
            run_requests.append(
                dict(
                    run_key=filepath,
                    run_config=RunConfig(
                        ops={
                            "master_csv_to_gold": file_config,
                        }
                    ),
                )
            )

    for request in run_requests:
        yield RunRequest(**request)


@sensor(
    job=school_reference__convert_gold_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_reference__gold_csv_to_deltatable_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.gold_folder}/reference")
    run_requests = []

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if dataset_type is None:
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]
            file_config = FileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema=f"gold_reference_{dataset_type.replace('-', '_')}",
            )
            run_requests.append(
                dict(
                    run_key=filepath,
                    run_config=RunConfig(
                        ops={
                            "reference_csv_to_gold": file_config,
                        }
                    ),
                )
            )

    for request in run_requests:
        yield RunRequest(**request)


@sensor(
    job=qos__convert_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def qos__csv_to_deltatable_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.gold_folder}/qos")
    run_requests = []

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]
            file_config = FileConfig(
                filepath=filepath,
                dataset_type="qos",
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema="qos",
            )
            run_requests.append(
                dict(
                    run_key=filepath,
                    run_config=RunConfig(
                        ops={
                            "qos_csv_to_gold": file_config,
                        }
                    ),
                )
            )

    for request in run_requests:
        yield RunRequest(**request)
