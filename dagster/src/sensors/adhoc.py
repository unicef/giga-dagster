from dagster import RunConfig, RunRequest, sensor
from src.constants import constants
from src.jobs.adhoc import (
    school_master__convert_gold_csv_to_deltatable_job,
    school_qos_bra__convert_csv_to_deltatable_job,
    school_reference__convert_gold_csv_to_deltatable_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient

from .base import FileConfig


@sensor(
    job=school_master__convert_gold_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master__gold_csv_to_deltatable_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.gold_source_folder}/master")
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
                dataset_type="master",
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema="school_master",
            )

            run_requests.append(
                {
                    "run_key": filepath,
                    "run_config": RunConfig(
                        ops={
                            "adhoc__load_master_csv": file_config,
                            "adhoc__master_data_quality_checks": file_config,
                            "adhoc__master_dq_checks_passed": file_config,
                            "adhoc__master_dq_checks_failed": file_config,
                            "adhoc__publish_master_to_gold": file_config,
                        }
                    ),
                }
            )

    for request in run_requests:
        yield RunRequest(**request)


@sensor(
    job=school_reference__convert_gold_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_reference__gold_csv_to_deltatable_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.gold_source_folder}/reference")
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
                dataset_type="reference",
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema="school_reference",
            )
            run_requests.append(
                {
                    "run_key": filepath,
                    "run_config": RunConfig(
                        ops={
                            "adhoc__load_reference_csv": file_config,
                            "adhoc__reference_data_quality_checks": file_config,
                            "adhoc__reference_dq_checks_passed": file_config,
                            "adhoc__reference_dq_checks_failed": file_config,
                            "adhoc__publish_reference_to_gold": file_config,
                        }
                    ),
                }
            )

    for request in run_requests:
        yield RunRequest(**request)


@sensor(
    job=school_qos_bra__convert_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def qos__csv_to_deltatable_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.gold_source_folder}/qos")
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
                {
                    "run_key": filepath,
                    "run_config": RunConfig(
                        ops={"adhoc__publish_qos_to_gold": file_config}
                    ),
                }
            )

    for request in run_requests:
        yield RunRequest(**request)