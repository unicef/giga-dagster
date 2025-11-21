from datetime import datetime
from pathlib import Path

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession

from dagster import (
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from src.constants import DataTier, constants
from src.jobs.adhoc import (
    custom_dataset_create_bronze_job,
    health_master__convert_gold_csv_to_deltatable_job,
    school_master__convert_gold_csv_to_deltatable_job,
    school_master__dq_checks_job,
    school_master__generate_silver_tables_job,
    school_qos__convert_csv_to_deltatable_job,
    school_qos_raw__convert_csv_to_deltatable_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.filename import (
    deconstruct_adhoc_filename_components,
)
from src.utils.op_config import (
    DatasetConfig,
    FileConfig,
    OpDestinationMapping,
    generate_run_ops,
)

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

        adls_filepath: str = file_data.name
        path = Path(adls_filepath)

        reference_parent = "updated_master_schema/reference"
        reference_parent = Path(reference_parent)
        path_name_country_code = path.name.split("_")[0]
        reference_name = f"{path_name_country_code}_master_reference.csv"
        reference_path = reference_parent / reference_name
        reference_stem = reference_path.stem

        stem = path.stem
        metadata = adls_file_client.fetch_metadata_for_blob(adls_filepath) or {}
        props = adls_file_client.get_file_metadata(filepath=adls_filepath)
        size = props.size
        master_metastore_schema = "school_master"
        reference_metastore_schema = "school_reference"

        filename_components = deconstruct_adhoc_filename_components(file_data.name)
        country_code = filename_components.country_code

        if not stem.startswith(filename_components.country_code):
            stem = f"{filename_components.country_code}_{stem}"

        ops_destination_mapping = {
            "adhoc__load_master_csv": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=master_metastore_schema,
                tier=DataTier.RAW,
            ),
            "adhoc__load_reference_csv": OpDestinationMapping(
                source_filepath=str(reference_path),
                destination_filepath=str(reference_path),
                metastore_schema=reference_metastore_schema,
                tier=DataTier.RAW,
            ),
            "adhoc__master_data_transforms": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                metastore_schema=master_metastore_schema,
                tier=DataTier.TRANSFORMS,
            ),
            "adhoc__df_duplicates": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/duplicates/{stem}.csv",
                metastore_schema=master_metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__master_data_quality_checks": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/transforms/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                metastore_schema=master_metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__reference_data_quality_checks": OpDestinationMapping(
                source_filepath=str(reference_path),
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{reference_stem}.csv",
                metastore_schema=reference_metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__master_dq_checks_summary": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/summary/{stem}.csv",
                metastore_schema=master_metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__master_dq_checks_passed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/passed/{stem}.csv",
                metastore_schema=master_metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__reference_dq_checks_passed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{reference_stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/passed/{reference_stem}.csv",
                metastore_schema=reference_metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__master_dq_checks_failed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-master/failed/{stem}.csv",
                metastore_schema=master_metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__reference_dq_checks_failed": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/full/{reference_stem}.csv",
                destination_filepath=f"{constants.gold_folder}/dq-results/school-reference/failed/{reference_stem}.csv",
                metastore_schema=reference_metastore_schema,
                tier=DataTier.DATA_QUALITY_CHECKS,
            ),
            "adhoc__publish_silver_geolocation": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_silver.db/{country_code.lower()}",
                metastore_schema="school_geolocation",
                tier=DataTier.SILVER,
            ),
            "adhoc__publish_silver_coverage": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/full/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_coverage_silver.db/{country_code.lower()}",
                metastore_schema="school_coverage",
                tier=DataTier.SILVER,
            ),
            "adhoc__publish_master_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/passed/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{master_metastore_schema}.db/{country_code}",
                metastore_schema=master_metastore_schema,
                tier=DataTier.GOLD,
            ),
            "adhoc__publish_reference_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-reference/passed/{reference_stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{reference_metastore_schema}.db/{country_code}",
                metastore_schema=reference_metastore_schema,
                tier=DataTier.GOLD,
            ),
            "adhoc__reset_geolocation_staging_table": OpDestinationMapping(
                source_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_staging.db/{country_code.lower()}",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_staging.db/{country_code.lower()}",
                metastore_schema="school_geolocation",
                tier=DataTier.STAGING,
            ),
            "adhoc__reset_coverage_staging_table": OpDestinationMapping(
                source_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_coverage_staging.db/{country_code.lower()}",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_coverage_staging.db/{country_code.lower()}",
                metastore_schema="school_coverage",
                tier=DataTier.STAGING,
            ),
            "adhoc__broadcast_master_release_notes": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/school-master/passed/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{master_metastore_schema}.db/{country_code}",
                metastore_schema=master_metastore_schema,
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
            dq_target_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{master_metastore_schema}.db/{country_code}",
        )

        last_modified = props.last_modified.strftime("%Y%m%d-%H%M%S")

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
    job=health_master__convert_gold_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def health_master__gold_csv_to_deltatable_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    run_requests = []

    for file_data in adls_file_client.list_paths_generator(
        f"{constants.gold_source_folder}/health-master", recursive=True
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

        country_code = path.parent.name
        stem = path.stem
        metadata = adls_file_client.fetch_metadata_for_blob(adls_filepath) or {}
        props = adls_file_client.get_file_metadata(filepath=adls_filepath)
        size = props.size
        metastore_schema = "health_master"

        ops_destination_mapping = {
            "adhoc__load_health_master_csv": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
            "adhoc__health_master_data_transforms": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/health-master/transforms/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.TRANSFORMS,
            ),
            "adhoc__publish_health_master_to_gold": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}.db/{country_code}",
                metastore_schema=metastore_schema,
                tier=DataTier.GOLD,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="health-master",
            metadata=metadata,
            file_size_bytes=size,
            domain="health",
            country_code=country_code,
        )

        last_modified = props.last_modified.strftime("%Y%m%d-%H%M%S")
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
    job=school_qos_raw__convert_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_qos_raw__gold_csv_to_deltatable_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    run_requests = []

    for file_data in adls_file_client.list_paths_generator(
        constants.qos_raw_source_folder, recursive=True
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
        metadata = adls_file_client.fetch_metadata_for_blob(adls_filepath) or {}
        props = adls_file_client.get_file_metadata(filepath=adls_filepath)
        size = props.size
        metastore_schema = "qos_raw"

        ops_destination_mapping = {
            "adhoc__load_qos_raw_csv": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema=metastore_schema,
                tier=DataTier.RAW,
            ),
            "adhoc__qos_raw_transforms": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.gold_folder}/dq-results/qos-raw/transforms/{country_code}/{stem}.csv",
                metastore_schema=metastore_schema,
                tier=DataTier.TRANSFORMS,
            ),
            "adhoc__publish_qos_raw_to_gold": OpDestinationMapping(
                source_filepath=f"{constants.gold_folder}/dq-results/qos-raw/transforms/{country_code}/{stem}.csv",
                destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}.db/{country_code}",
                metastore_schema=metastore_schema,
                tier=DataTier.GOLD,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="qos-raw",
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


# @sensor(
#     job=school_qos__convert_csv_to_deltatable_job,
#     minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
# )
# def school_qos__gold_csv_to_deltatable_sensor(
#     context: SensorEvaluationContext,
#     adls_file_client: ADLSFileClient,
# ):
#     run_requests = []

#     for file_data in adls_file_client.list_paths_generator(
#         constants.qos_source_folder, recursive=True
#     ):
#         adls_filepath = file_data.name
#         path = Path(adls_filepath)

#         if (
#             file_data.is_directory
#             or len(path.parent.name) != 3
#             or path.suffix.lower() != ".csv"
#         ):
#             context.log.warning(f"Skipping {adls_filepath}")
#             continue

#         stem = path.stem
#         country_code = path.parent.name
#         properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
#         metadata = properties.metadata
#         size = properties.size
#         metastore_schema = "qos"

#         ops_destination_mapping = {
#             "adhoc__load_qos_csv": OpDestinationMapping(
#                 source_filepath=str(path),
#                 destination_filepath=str(path),
#                 metastore_schema=metastore_schema,
#                 tier=DataTier.RAW,
#             ),
#             "adhoc__qos_transforms": OpDestinationMapping(
#                 source_filepath=str(path),
#                 destination_filepath=f"{constants.gold_folder}/dq-results/qos/transforms/{country_code}/{stem}.csv",
#                 metastore_schema=metastore_schema,
#                 tier=DataTier.TRANSFORMS,
#             ),
#             "adhoc__publish_qos_to_gold": OpDestinationMapping(
#                 source_filepath=f"{constants.gold_folder}/dq-results/qos/transforms/{country_code}/{stem}.csv",
#                 destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/{metastore_schema}.db/{country_code}",
#                 metastore_schema=metastore_schema,
#                 tier=DataTier.GOLD,
#             ),
#         }

#         run_ops = generate_run_ops(
#             ops_destination_mapping,
#             dataset_type="qos",
#             metadata=metadata,
#             file_size_bytes=size,
#             domain=DOMAIN,
#             country_code=country_code,
#         )

#         context.log.info(f"FILE: {path}")
#         run_requests.append(
#             RunRequest(
#                 run_key=str(path),
#                 run_config=RunConfig(ops=run_ops),
#                 tags={"country": country_code},
#             ),
#         )

#     if len(run_requests) == 0:
#         yield SkipReason("No files found to process.")
#     else:
#         yield from run_requests


@sensor(
    job=school_qos__convert_csv_to_deltatable_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_qos__gold_csv_to_deltatable_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    # ensure last_processed_time is initialized correctly
    if isinstance(context.cursor, str) and context.cursor:
        try:
            last_processed_time = datetime.fromisoformat(context.cursor)
        except ValueError:
            context.log.warning(
                f"Invalid cursor format: {context.cursor}, resetting to epoch."
            )
            last_processed_time = datetime.min
    else:
        last_processed_time = datetime.min  # default to the beginning of time
    new_last_processed_time = last_processed_time
    new_files_found = False

    for file_data in adls_file_client.list_paths_generator(
        constants.qos_source_folder, recursive=True
    ):
        adls_filepath = file_data.name
        path = Path(adls_filepath)

        # skip directories, invalid paths, or non-CSV files
        if (
            file_data.is_directory
            or len(path.parent.name) != 3
            or path.suffix.lower() != ".csv"
        ):
            context.log.warning(f"Skipping {adls_filepath}")
            continue

        # ensure file_data.last_modified is a valid datetime object
        file_modified_time = file_data.last_modified
        if not isinstance(file_modified_time, datetime):
            context.log.warning(
                f"Skipping file {adls_filepath} due to missing or invalid last_modified timestamp."
            )
            continue

        # skip files that were already processed
        if file_modified_time <= last_processed_time:
            continue

        # process new file
        stem = path.stem
        country_code = path.parent.name
        metadata = adls_file_client.fetch_metadata_for_blob(adls_filepath) or {}
        props = adls_file_client.get_file_metadata(filepath=adls_filepath)
        size = props.size
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

        context.log.info(f"Processing new file: {adls_filepath}")

        yield RunRequest(
            run_key=str(path),
            run_config=RunConfig(ops=run_ops),
            tags={"country": country_code},
        )

        # update the latest processed file timestamp
        new_last_processed_time = max(new_last_processed_time, file_modified_time)
        new_files_found = True

    # update cursor with the latest processed timestamp
    if new_files_found:
        context.update_cursor(new_last_processed_time.isoformat())
    else:
        yield SkipReason("No new files found to process.")


@sensor(
    job=school_master__generate_silver_tables_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master__generate_silver_tables_sensor(
    context: SensorEvaluationContext, spark: PySparkResource
):
    s: SparkSession = spark.spark_session
    gold_master_tables = s.catalog.listTables("school_master")

    for table in gold_master_tables:
        context.log.info(f"TABLE: {table.name}")

        yield RunRequest(
            run_key=table.name,
            run_config=RunConfig(
                ops={
                    "adhoc__generate_silver_geolocation_from_gold": DatasetConfig(
                        country_code=table.name.lower()
                    ),
                    "adhoc__generate_silver_coverage_from_gold": DatasetConfig(
                        country_code=table.name.lower()
                    ),
                }
            ),
            tags={"country": table.name.upper()},
        )


@sensor(
    job=school_master__dq_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master__dq_checks_sensor(
    context: SensorEvaluationContext, spark: PySparkResource
):
    s: SparkSession = spark.spark_session
    gold_master_tables = s.catalog.listTables("school_master")

    for table in gold_master_tables:
        context.log.info(f"TABLE: {table.name}")

        yield RunRequest(
            run_key=table.name,
            run_config=RunConfig(
                ops={
                    "adhoc__standalone_master_data_quality_checks": FileConfig(
                        country_code=table.name.upper(),
                        dataset_type="master",
                        filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_master.db/{table.name.upper()}",
                        destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_master.db/{table.name.upper()}",
                        dq_target_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_master.db/{table.name.upper()}",
                        domain=DOMAIN,
                        file_size_bytes=0,
                        metastore_schema="school_master",
                        tier=DataTier.GOLD,
                    )
                }
            ),
            tags={"country": table.name.upper()},
        )


@sensor(
    job=custom_dataset_create_bronze_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def custom_dataset_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = f"{constants.raw_folder}/custom-dataset"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        metadata = adls_file_client.fetch_metadata_for_blob(adls_filepath) or {}
        props = adls_file_client.get_file_metadata(filepath=adls_filepath)
        size = props.size

        ops_destination_mapping = {
            "custom_dataset_raw": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=str(path),
                metastore_schema="custom_dataset",
                tier=DataTier.RAW,
            ),
            "custom_dataset_bronze": OpDestinationMapping(
                source_filepath=str(path),
                destination_filepath=f"{constants.bronze_folder}/custom_dataset.db/{stem}",
                metastore_schema="custom_dataset",
                tier=DataTier.BRONZE,
            ),
        }

        run_ops = generate_run_ops(
            ops_destination_mapping,
            dataset_type="custom",
            metadata=metadata,
            file_size_bytes=size,
            domain="custom",
            country_code=stem,
        )

        last_modified = props.last_modified.strftime("%Y%m%d-%H%M%S")
        context.log.info(f"FILE: {path}")
        yield RunRequest(
            run_key=f"{path}:{last_modified}",
            run_config=RunConfig(ops=run_ops),
        )
        count += 1

    if count == 0:
        yield SkipReason(f"No uploads detected in {source_directory}")
