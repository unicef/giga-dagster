import os.path

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
        if file_data.is_directory:
            continue
        else:
            filepath = file_data.name
            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties.metadata
            size = properties.size
            file_config = FileConfig(
                filepath=filepath,
                dataset_type="master",
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema="school_master",
            )

            ops_list = [
                "adhoc__load_master_csv",
                "adhoc__master_data_transforms",
                "adhoc__df_duplicates",
                "adhoc__master_data_quality_checks",
                "adhoc__master_dq_checks_summary",
                "adhoc__master_dq_checks_passed",
                "adhoc__master_dq_checks_failed",
                "adhoc__publish_master_to_gold",
            ]

            run_requests.append(
                {
                    "run_key": filepath,
                    "run_config": RunConfig(
                        ops=dict(
                            zip(ops_list, [file_config] * len(ops_list), strict=True)
                        )
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
        if file_data.is_directory:
            continue
        else:
            filepath = file_data.name
            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties.metadata
            size = properties.size
            file_config = FileConfig(
                filepath=filepath,
                dataset_type="reference",
                metadata=metadata,
                file_size_bytes=size,
                metastore_schema="school_reference",
            )

            ops_list = [
                "adhoc__load_reference_csv",
                "adhoc__reference_data_quality_checks",
                "adhoc__reference_dq_checks_passed",
                "adhoc__reference_dq_checks_failed",
                "adhoc__publish_reference_to_gold",
            ]

            run_requests.append(
                {
                    "run_key": filepath,
                    "run_config": RunConfig(
                        ops=dict(
                            zip(ops_list, [file_config] * len(ops_list), strict=True)
                        )
                    ),
                }
            )

    for request in run_requests:
        yield RunRequest(**request)


@sensor(
    job=school_qos_bra__convert_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_qos_bra__gold_csv_to_deltatable_sensor():
    adls = ADLSFileClient()

    file_list = sorted(
        [
            p
            for p in adls.list_paths(f"{constants.qos_source_folder}/BRA")
            if not p.is_directory and os.path.splitext(p.name)[1] == ".csv"
        ],
        key=lambda x: x.last_modified,
    )
    run_requests = []

    for file_data in file_list:
        filepath = file_data.name
        properties = adls.get_file_metadata(filepath=filepath)
        file_config = FileConfig(
            filepath=filepath,
            dataset_type="qos",
            metadata=properties.metadata,
            file_size_bytes=properties.size,
            metastore_schema="qos",
            unique_identifier_column="gigasync_id",
            partition_columns=["date"],
        )
        run_requests.append(
            {
                "run_key": filepath,
                "run_config": RunConfig(
                    ops={
                        "adhoc__load_qos_bra_csv": file_config,
                        "adhoc__qos_bra_transforms": file_config,
                        "adhoc__publish_qos_bra_to_gold": file_config,
                    }
                ),
            }
        )

    for request in run_requests:
        yield RunRequest(**request)
