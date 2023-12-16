from dagster import Config, RunConfig, RunRequest, sensor
from src.constants import constants
from src.jobs import (
    school_master__automated_data_checks_job,
    school_master__convert_gold_csv_to_deltatable_job,
    school_master__failed_manual_checks_job,
    school_master__successful_manual_checks_job,
    school_reference__convert_gold_csv_to_deltatable_job,
)
from src.utils.adls import ADLSFileClient


class FileConfig(Config):
    filepath: str
    dataset_type: str
    metadata: dict
    file_size_bytes: int
    metastore_schema: str
    table_schema_definition: str


def get_dataset_type(filepath: str) -> str | None:
    if "geolocation" in filepath:
        return "school-geolocation-data"
    elif "coverage" in filepath:
        return "school-coverage-data"
    elif "reference" in filepath:
        return "school-reference"
    else:
        return None


@sensor(job=school_master__automated_data_checks_job, minimum_interval_seconds=30)
def school_master__raw_file_uploads_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.raw_folder}")

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

            get_file_config = lambda layer: FileConfig(  # noqa: E731
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
                # TODO: Add the correct metastore schema and table SQL definition
                metastore_schema=f"{layer}_{dataset_type.replace('-', '_')}",
            )

            print(f"FILE: {filepath}")

            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "raw": get_file_config("raw"),
                        "bronze": get_file_config("bronze"),
                        "data_quality_results": get_file_config("data_quality_results"),
                        "dq_passed_rows": get_file_config("dq_passed_rows"),
                        "dq_failed_rows": get_file_config("dq_failed_rows"),
                    }
                ),
            )


@sensor(job=school_master__successful_manual_checks_job, minimum_interval_seconds=60)
def school_master__successful_manual_checks_sensor():
    adls = ADLSFileClient()

    file_list = adls.list_paths(f"{constants.staging_approved_folder}")

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

            get_file_config = lambda layer: FileConfig(  # noqa: E731
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=size,
                # TODO: Add the correct metastore schema and table SQL definition
                metastore_schema=(
                    f"manual_check_success_{dataset_type.replace('-', '_')}"
                ),
            )

            print(f"FILE: {filepath}")
            yield RunRequest(
                run_key=f"{filepath}",
                run_config=RunConfig(
                    ops={
                        "manual_review_passed_rows": get_file_config(
                            "manual_review_passed"
                        ),
                        "silver": get_file_config("silver"),
                        "gold": get_file_config("gold"),
                    }
                ),
            )


@sensor(job=school_master__failed_manual_checks_job, minimum_interval_seconds=60)
def school_master__failed_manual_checks_sensor():
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
    job=school_master__convert_gold_csv_to_deltatable_job, minimum_interval_seconds=30
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
                table_schema_definition="create_school_master_table",
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
    minimum_interval_seconds=30,
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
                table_schema_definition="create_school_reference_table",
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
