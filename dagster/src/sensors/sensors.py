from dagster import RunConfig, RunRequest, sensor
from src.constants import constants
from src.jobs import (
    qos__convert_csv_to_deltatable_job,
    school_master__convert_gold_csv_to_deltatable_job,
    school_master_coverage__automated_data_checks_job,
    school_master_coverage__failed_manual_checks_job,
    school_master_coverage__successful_manual_checks_job,
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__failed_manual_checks_job,
    school_master_geolocation__successful_manual_checks_job,
    school_reference__convert_gold_csv_to_deltatable_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient

from .config import FileConfig


def get_dataset_type(filepath: str) -> str | None:
    if "master" in filepath:
        return "master"
    elif "reference" in filepath:
        return "reference"
    elif "qos" in filepath:
        return "qos"
    elif "geolocation" in filepath:
        return "geolocation"
    elif "coverage" in filepath:
        return "coverage"
    else:
        return None


@sensor(
    job=school_master_geolocation__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__raw_file_uploads_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.raw_folder}")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if (
                dataset_type != "geolocation"
                or "test_pipeline" not in filepath.split("/")[-1]
            ):
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]

            file_config_params = {
                "filepath": filepath,
                "dataset_type": "geolocation",
                "metadata": metadata,
                "file_size_bytes": size,
            }

            get_file_config = lambda layer, params: FileConfig(  # noqa: E731
                **params,
                # TODO: Add the correct metastore schema and table SQL definition
                metastore_schema=f"{layer}_{params['dataset_type'].replace('-', '_')}",
            )

            print(f"FILE: {filepath}")

            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "geolocation_raw": get_file_config(
                            "geolocation_raw", file_config_params
                        ),
                        "geolocation_bronze": get_file_config(
                            "geolocation_bronze", file_config_params
                        ),
                        "geolocation_data_quality_results": get_file_config(
                            "geolocation_data_quality_results", file_config_params
                        ),
                        "geolocation_dq_passed_rows": get_file_config(
                            "geolocation_dq_passed_rows", file_config_params
                        ),
                        "geolocation_dq_failed_rows": get_file_config(
                            "geolocation_dq_failed_rows", file_config_params
                        ),
                        # "geolocation_staging": get_file_config(
                        #     "geolocation_staging", file_config_params
                        # ),
                    }
                ),
            )


@sensor(
    job=school_master_coverage__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__raw_file_uploads_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.raw_folder}")

    for file_data in file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            dataset_type = get_dataset_type(filepath)
            if (
                dataset_type != "coverage"
                or "test_pipeline" not in filepath.split("/")[-1]
            ):
                continue

            properties = adls.get_file_metadata(filepath=filepath)
            metadata = properties["metadata"]
            size = properties["size"]

            file_config_params = {
                "filepath": filepath,
                "dataset_type": "coverage",
                "metadata": metadata,
                "file_size_bytes": size,
            }

            get_file_config = lambda layer, params: FileConfig(  # noqa: E731
                **params,
                # TODO: Add the correct metastore schema and table SQL definition
                metastore_schema=f"{layer}_{params['dataset_type'].replace('-', '_')}",
            )

            print(f"FILE: {filepath}")

            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "coverage_raw": get_file_config(
                            "coverage_raw", file_config_params
                        ),
                        "coverage_data_quality_results": get_file_config(
                            "coverage_data_quality_results", file_config_params
                        ),
                        "coverage_dq_passed_rows": get_file_config(
                            "coverage_dq_passed_rows", file_config_params
                        ),
                        "coverage_dq_failed_rows": get_file_config(
                            "coverage_dq_failed_rows", file_config_params
                        ),
                        "coverage_bronze": get_file_config(
                            "coverage_bronze", file_config_params
                        ),
                        # "coverage_staging": get_file_config(
                        #     "coverage_staging", file_config_params
                        # ),
                    }
                ),
            )


@sensor(
    job=school_master_geolocation__successful_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__successful_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.dq_passed_folder}/school-geolocation-data")

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

            file_config_params = {
                "filepath": filepath,
                "dataset_type": dataset_type,
                "metadata": metadata,
                "file_size_bytes": size,
            }

            get_file_config = lambda layer, params: FileConfig(  # noqa: E731
                **params,
                # TODO: Add the correct metastore schema and table SQL definition
                metastore_schema=(
                    f"manual_check_success_{params['dataset_type'].replace('-', '_')}"
                ),
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_passed_rows": get_file_config(
                            "manual_review_passed", file_config_params
                        ),
                        "silver": get_file_config("silver", file_config_params),
                        "gold": get_file_config("gold", file_config_params),
                    }
                ),
            )


@sensor(
    job=school_master_coverage__successful_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__successful_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.dq_passed_folder}/school-coverage-data")

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

            file_config_params = {
                "filepath": filepath,
                "dataset_type": dataset_type,
                "metadata": metadata,
                "file_size_bytes": size,
            }

            get_file_config = lambda layer, params: FileConfig(  # noqa: E731
                **params,
                # TODO: Add the correct metastore schema and table SQL definition
                metastore_schema=(
                    f"manual_check_success_{params['dataset_type'].replace('-', '_')}"
                ),
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_passed_rows": get_file_config(
                            "manual_review_passed", file_config_params
                        ),
                        "silver": get_file_config("silver", file_config_params),
                        "gold": get_file_config("gold", file_config_params),
                    }
                ),
            )


@sensor(
    job=school_master_geolocation__failed_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__failed_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.archive_manual_review_rejected_folder}")

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
                # TODO: Add the correct metastore schema and table SQL definition
                metastore_schema=(
                    f"manual_review_failed_{dataset_type.replace('-', '_')}"
                ),
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_failed_rows": file_config,
                    }
                ),
            )


@sensor(
    job=school_master_coverage__failed_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_coverage__failed_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.archive_manual_review_rejected_folder}")

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
                # TODO: Add the correct metastore schema and table SQL definition
                metastore_schema=(
                    f"manual_review_failed_{dataset_type.replace('-', '_')}"
                ),
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_failed_rows": file_config,
                    }
                ),
            )


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
                            "adhoc__publish_reference_to_gold": file_config,
                        }
                    ),
                }
            )

    for request in run_requests:
        yield RunRequest(**request)


@sensor(
    job=qos__convert_csv_to_deltatable_job,
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