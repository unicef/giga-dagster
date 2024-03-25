from pathlib import Path

from dagster import (
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from src.constants import constants
from src.jobs.adhoc import (
    school_master__convert_gold_csv_to_deltatable_job,
    school_qos__convert_csv_to_deltatable_job,
    school_reference__convert_gold_csv_to_deltatable_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.op_config import OpDestinationMapping, generate_run_ops


@sensor(
    job=school_master__convert_gold_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master__gold_csv_to_deltatable_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    paths_list = adls_file_client.list_paths(
        f"{constants.gold_source_folder}/master", recursive=False
    )
    run_requests = []

    for file_data in paths_list:
        if file_data.is_directory:
            context.log.warning(f"Skipping {file_data.name}")
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_master"

        ops_destination_mapping = {
            "adhoc__load_master_csv": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
            ),
            "adhoc__master_data_transforms": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__df_duplicates": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/duplicates/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__master_data_quality_checks": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__master_dq_checks_summary": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/summary/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__master_dq_checks_passed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/passed/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__master_dq_checks_failed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/failed/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__publish_master_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/passed/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/school-master/{stem}",
                metastore_schema=metastore_schema,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="master",
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {path}")
        run_requests.append(
            RunRequest(run_key=str(path), run_config=RunConfig(ops=run_ops))
        )

    yield from run_requests


@sensor(
    job=school_reference__convert_gold_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_reference__gold_csv_to_deltatable_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    paths_list = adls_file_client.list_paths(
        f"{constants.gold_source_folder}/reference", recursive=False
    )
    run_requests = []

    for file_data in paths_list:
        if file_data.is_directory:
            context.log.warning(f"Skipping {file_data.name}")
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "school_reference"

        ops_destination_mapping = {
            "adhoc__load_reference_csv": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
            ),
            "adhoc__reference_data_quality_checks": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__reference_dq_checks_passed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/passed/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__reference_dq_checks_failed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/failed/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__publish_reference_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/passed/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/school-reference/{stem}",
                metastore_schema=metastore_schema,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="reference",
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {path}")
        run_requests.append(
            RunRequest(run_key=str(path), run_config=RunConfig(ops=run_ops))
        )

    yield from run_requests


@sensor(
    job=school_qos__convert_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_qos__gold_csv_to_deltatable_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    run_requests = []

    for file_data in adls_file_client.list_paths_generator(
        constants.qos_source_folder, recursive=True
    ):
        adls_filepath = file_data.name
        path = Path(adls_filepath)

        if (
            file_data.is_directory
            or len(path.parent.name) != 3
            or path.suffix.lower() != ".csv"
        ):
            context.log.warning(f"Skipping {adls_filepath}")
            continue

        stem = path.stem
        country_code = path.parent.name
        properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
        metadata = properties.metadata
        size = properties.size
        metastore_schema = "qos"

        ops_destination_mapping = {
            "adhoc__load_qos_csv": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
            ),
            "adhoc__qos_transforms": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/qos/transforms/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
            ),
            "adhoc__publish_qos_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/qos/transforms/{country_code}/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/qos/{country_code}/{stem}",
                metastore_schema=metastore_schema,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="qos",
            metadata=metadata,
            file_size_bytes=size,
        )

        context.log.info(f"FILE: {path}")
        run_requests.append(
            RunRequest(run_key=str(path), run_config=RunConfig(ops=run_ops))
        )

    if len(run_requests) == 0:
        yield SkipReason("No files found to process.")
    else:
        yield from run_requests
