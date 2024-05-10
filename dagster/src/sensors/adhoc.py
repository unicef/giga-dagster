from pathlib import Path

from dagster import (
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from src.constants import DataTier, constants
from src.jobs.adhoc import (
    school_master__convert_gold_csv_to_deltatable_job,
    school_master__generate_silver_job,
    school_qos__convert_csv_to_deltatable_job,
    school_reference__convert_gold_csv_to_deltatable_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.filename import (
    deconstruct_adhoc_filename_components,
)
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DOMAIN = "school"


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
    paths_list.extend(
        adls_file_client.list_paths(
            constants.adhoc_master_updates_source_folder, recursive=True
        ),
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

        filename_components = deconstruct_adhoc_filename_components(file_data.name)
        country_code = filename_components.country_code

        if not stem.startswith(filename_components.country_code):
            stem = f"{filename_components.country_code}_{stem}"

        ops_destination_mapping = {
            "adhoc__load_master_csv": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
            "adhoc__master_data_transforms": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.TRANSFORMS,
            ),
            "adhoc__df_duplicates": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/duplicates/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__master_data_quality_checks": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__master_dq_checks_summary": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/summary/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__master_dq_checks_passed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/passed/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__master_dq_checks_failed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/failed/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__publish_master_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/passed/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}.db/{country_code}",
                metastore_schema=metastore_schema,
                tier=DataTier.GOLD,
            ),
            "adhoc__broadcast_master_release_notes": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/passed/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}.db/{country_code}",
                metastore_schema=metastore_schema,
                tier=DataTier.GOLD,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="master",
            metadata=metadata,
            file_size_bytes=size,
            domain=DOMAIN,
            country_code=country_code,
            dq_target_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}.db/{country_code}",
        )

        last_modified = properties.last_modified.strftime("%Y%m%d-%H%M%S")

        context.log.info(f"FILE: {path}")
        run_requests.append(
            RunRequest(
                run_key=f"{path}:{last_modified}",
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            ),
        )

    if len(run_requests) == 0:
        yield SkipReason("No files found to process.")
    else:
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

        filename_components = deconstruct_adhoc_filename_components(file_data.name)
        country_code = filename_components.country_code

        if not stem.startswith(filename_components.country_code):
            stem = f"{filename_components.country_code}_{stem}"

        ops_destination_mapping = {
            "adhoc__load_reference_csv": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
            "adhoc__reference_data_quality_checks": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__reference_dq_checks_passed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/passed/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__reference_dq_checks_failed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/failed/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__publish_reference_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/passed/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}.db/{country_code}",
                metastore_schema=metastore_schema,
                tier=DataTier.GOLD,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="reference",
            metadata=metadata,
            file_size_bytes=size,
            domain=DOMAIN,
            country_code=country_code,
        )

        last_modified = properties.last_modified.strftime("%Y%m%d-%H%M%S")

        context.log.info(f"FILE: {path}")
        run_requests.append(
            RunRequest(
                run_key=f"{path}:{last_modified}",
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            ),
        )

    if len(run_requests) == 0:
        yield SkipReason("No files found to process.")
    else:
        yield from run_requests


@sensor(
    job=school_master__generate_silver_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master__generate_silver_sensor(
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

        filename_components = deconstruct_adhoc_filename_components(file_data.name)
        country_code = filename_components.country_code

        if not stem.startswith(filename_components.country_code):
            stem = f"{filename_components.country_code}_{stem}"

        ops_destination_mapping = {
            "adhoc__generate_silver_geolocation": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_silver.db/{country_code.lower()}",
                metastore_schema="school_geolocation",
                tier=DataTier.SILVER,
            ),
            "adhoc__generate_silver_coverage": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_coverage_silver.db/{country_code.lower()}",
                metastore_schema="school_coverage",
                tier=DataTier.SILVER,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="silver",
            metadata=metadata,
            file_size_bytes=size,
            domain=DOMAIN,
            country_code=country_code,
        )

        context.log.info(f"FILE: {path}")
        run_requests.append(
            RunRequest(
                run_key="generate_silver_for_" + str(path),
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            ),
        )

    if len(run_requests) == 0:
        yield SkipReason("No files found to process.")
    else:
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
                tier=DataTier.RAW,
            ),
            "adhoc__qos_transforms": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/qos/transforms/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.TRANSFORMS,
            ),
            "adhoc__publish_qos_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/qos/transforms/{country_code}/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}.db/{country_code}",
                metastore_schema=metastore_schema,
                tier=DataTier.GOLD,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="qos",
            metadata=metadata,
            file_size_bytes=size,
            domain=DOMAIN,
            country_code=country_code,
        )

        context.log.info(f"FILE: {path}")
        run_requests.append(
            RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            ),
        )

    if len(run_requests) == 0:
        yield SkipReason("No files found to process.")
    else:
        yield from run_requests
