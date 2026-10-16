from pathlib import Path

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor

from src.constants import DataTier, constants
from src.facilities import HEALTH_SPEC, FacilityGeolocationSpec
from src.jobs.facility_geolocation import (
    health_facility_geolocation__admin_delete_rows_job,
    health_facility_geolocation__automated_data_checks_job,
    health_facility_geolocation__post_manual_checks_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.filename import deconstruct_facility_filename_components
from src.utils.op_config import OpDestinationMapping, generate_run_ops


def _automated_destinations(
    spec: FacilityGeolocationSpec,
    path: Path,
    country_code: str,
) -> dict[str, OpDestinationMapping]:
    prefix = spec.assets.raw.removesuffix("_raw")
    stem = path.stem
    dq_root = f"{constants.dq_results_folder}/{spec.paths.dq_directory}"
    bronze_path = (
        f"{constants.bronze_folder}/{spec.paths.dq_directory}/"
        f"{country_code}/{stem}.parquet"
    )
    dq_path = f"{dq_root}/dq-overall/{country_code}/{stem}.parquet"
    common = {
        "metastore_schema": spec.metastore_schema,
        "tier": DataTier.DATA_QUALITY_CHECKS,
    }
    return {
        spec.assets.raw: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=str(path),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.RAW,
        ),
        f"{prefix}_metadata": OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath="",
            metastore_schema=spec.metastore_schema,
            tier=DataTier.BRONZE,
        ),
        spec.assets.bronze: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=bronze_path,
            metastore_schema=spec.metastore_schema,
            tier=DataTier.BRONZE,
        ),
        spec.assets.dq_results: OpDestinationMapping(
            source_filepath=bronze_path,
            destination_filepath=dq_path,
            output_filepaths={
                spec.assets.dq_results: dq_path,
                f"{prefix}_dq_duplicates_report": (
                    f"{dq_root}/dq-duplicates-report/{country_code}/{stem}.csv"
                ),
            },
            **common,
        ),
        f"{prefix}_human_readable": OpDestinationMapping(
            source_filepath=dq_path,
            destination_filepath="",
            output_filepaths={
                f"{prefix}_dq_passed_human_readable": (
                    f"{dq_root}/dq-passed-rows-human-readable/"
                    f"{country_code}/{stem}.csv"
                ),
                f"{prefix}_dq_failed_human_readable": (
                    f"{dq_root}/dq-failed-rows-human-readable/"
                    f"{country_code}/{stem}.csv"
                ),
            },
            **common,
        ),
        f"{prefix}_data_quality_results_summary": OpDestinationMapping(
            source_filepath=dq_path,
            destination_filepath=(
                f"{dq_root}/dq-summary/{country_code}/{stem}.json"
            ),
            **common,
        ),
        f"{prefix}_dq_passed_rows": OpDestinationMapping(
            source_filepath=dq_path,
            destination_filepath=(
                f"{dq_root}/dq-passed-rows/{country_code}/{stem}.parquet"
            ),
            **common,
        ),
        f"{prefix}_dq_failed_rows": OpDestinationMapping(
            source_filepath=dq_path,
            destination_filepath=(
                f"{dq_root}/dq-failed-rows/{country_code}/{stem}.parquet"
            ),
            **common,
        ),
        f"{prefix}_error_table": OpDestinationMapping(
            source_filepath=dq_path,
            destination_filepath="",
            **common,
        ),
        spec.assets.staging: OpDestinationMapping(
            source_filepath=dq_path,
            destination_filepath=(
                f"{settings.SPARK_WAREHOUSE_PATH}/"
                f"{spec.tables.staging_schema}.db/{country_code.lower()}"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.STAGING,
        ),
        f"{prefix}_map": OpDestinationMapping(
            source_filepath=dq_path,
            destination_filepath=(
                f"{dq_root}/dq-map/{country_code}/"
                f"{spec.facility_type}_facility_map_{country_code}_{stem}.html"
            ),
            **common,
        ),
        f"{prefix}_dq_kit_zip": OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{dq_root}/dq-kit/{country_code}/"
                f"DQ_Kit_{country_code}_{spec.facility_type}_{stem}.zip"
            ),
            **common,
        ),
    }


def _post_approval_destinations(
    spec: FacilityGeolocationSpec,
    path: Path,
    country_code: str,
) -> dict[str, OpDestinationMapping]:
    prefix = spec.assets.raw.removesuffix("_raw")
    warehouse = settings.SPARK_WAREHOUSE_PATH
    return {
        spec.assets.approved_rows: OpDestinationMapping(
            source_filepath=(
                f"{warehouse}/{spec.tables.staging_schema}.db/"
                f"{country_code.lower()}"
            ),
            destination_filepath=(
                f"{constants.staging_folder}/approved-facility-rows/"
                f"{spec.facility_type}/{country_code}/{path.stem}.parquet"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.STAGING,
        ),
        spec.assets.rejected_rows: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{warehouse}/{spec.tables.rejected_schema}.db/"
                f"{country_code.lower()}"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.MANUAL_REJECTED,
        ),
        spec.assets.silver: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{warehouse}/{spec.tables.silver_schema}.db/"
                f"{country_code.lower()}"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.SILVER,
            table_name=country_code.lower(),
        ),
        spec.assets.core: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{warehouse}/{spec.tables.core_schema}.db/{country_code.lower()}"
            ),
            metastore_schema=spec.tables.core_schema,
            tier=DataTier.GOLD,
            table_name=country_code.lower(),
        ),
        spec.assets.extension: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{warehouse}/{spec.tables.extension_schema}.db/"
                f"{country_code.lower()}"
            ),
            metastore_schema=spec.tables.extension_schema,
            tier=DataTier.GOLD,
            table_name=country_code.lower(),
        ),
        f"{prefix}_publish_complete": OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath="",
            metastore_schema=spec.metastore_schema,
            tier=DataTier.GOLD,
        ),
    }


@sensor(
    name="health_facility_geolocation__raw_file_uploads_sensor",
    job=health_facility_geolocation__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def health_facility_geolocation__raw_file_uploads_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    source = f"{constants.UPLOAD_PATH_PREFIX}/{HEALTH_SPEC.paths.upload_directory}"
    count = 0
    for file_data in adls_file_client.list_paths_generator(source, recursive=True):
        if file_data.is_directory:
            continue
        path = Path(file_data.name)
        try:
            components = deconstruct_facility_filename_components(str(path))
        except Exception as exc:
            context.log.error(f"Invalid facility filename {path}: {exc}")
            continue
        metadata = adls_file_client.fetch_metadata_for_blob(str(path), ensure_exists=True)
        properties = adls_file_client.get_file_metadata(filepath=str(path))
        run_ops = generate_run_ops(
            _automated_destinations(HEALTH_SPEC, path, components.country_code),
            dataset_type=HEALTH_SPEC.dataset_type,
            metadata={
                **metadata,
                "approval_dataset_label": HEALTH_SPEC.approval_label,
                "facility_type": HEALTH_SPEC.facility_type,
            },
            file_size_bytes=properties.size,
            domain=HEALTH_SPEC.domain,
            country_code=components.country_code,
        )
        yield RunRequest(
            run_key=str(path),
            run_config=RunConfig(ops=run_ops),
            tags={
                "country": components.country_code,
                "facility_type": HEALTH_SPEC.facility_type,
            },
        )
        count += 1
    if count == 0:
        yield SkipReason(f"No uploads detected in {source}")


@sensor(
    name="health_facility_geolocation__post_manual_checks_sensor",
    job=health_facility_geolocation__post_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def health_facility_geolocation__post_manual_checks_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    source = f"{constants.staging_folder}/{HEALTH_SPEC.paths.approval_directory}"
    count = 0
    for file_data in adls_file_client.list_paths_generator(source, recursive=True):
        if file_data.is_directory:
            continue
        path = Path(file_data.name)
        try:
            components = deconstruct_facility_filename_components(str(path))
        except Exception as exc:
            context.log.error(f"Invalid approval filename {path}: {exc}")
            continue
        metadata = adls_file_client.fetch_metadata_for_blob(str(path)) or {}
        run_ops = generate_run_ops(
            _post_approval_destinations(HEALTH_SPEC, path, components.country_code),
            dataset_type=HEALTH_SPEC.dataset_type,
            metadata={
                **metadata,
                "approval_dataset_label": HEALTH_SPEC.approval_label,
                "facility_type": HEALTH_SPEC.facility_type,
            },
            file_size_bytes=0,
            domain=HEALTH_SPEC.domain,
            country_code=components.country_code,
        )
        yield RunRequest(
            run_key=str(path),
            run_config=RunConfig(ops=run_ops),
            tags={
                "country": components.country_code,
                "facility_type": HEALTH_SPEC.facility_type,
            },
        )
        count += 1
    if count == 0:
        yield SkipReason(f"No approvals detected in {source}")


@sensor(
    name="health_facility_geolocation__admin_delete_rows_sensor",
    job=health_facility_geolocation__admin_delete_rows_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def health_facility_geolocation__admin_delete_rows_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    source = f"{constants.staging_folder}/{HEALTH_SPEC.paths.delete_directory}"
    count = 0
    for file_data in adls_file_client.list_paths_generator(source, recursive=True):
        if file_data.is_directory:
            continue
        path = Path(file_data.name)
        try:
            components = deconstruct_facility_filename_components(str(path))
        except Exception as exc:
            context.log.error(f"Invalid deletion filename {path}: {exc}")
            continue
        metadata = adls_file_client.fetch_metadata_for_blob(str(path)) or {}
        delete_asset = (
            f"{HEALTH_SPEC.assets.raw.removesuffix('_raw')}_delete_staging"
        )
        run_ops = generate_run_ops(
            {
                delete_asset: OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=(
                        f"{settings.SPARK_WAREHOUSE_PATH}/"
                        f"{HEALTH_SPEC.tables.staging_schema}.db/"
                        f"{components.country_code.lower()}"
                    ),
                    metastore_schema=HEALTH_SPEC.metastore_schema,
                    tier=DataTier.STAGING,
                )
            },
            dataset_type=HEALTH_SPEC.dataset_type,
            metadata={
                **metadata,
                "approval_dataset_label": HEALTH_SPEC.approval_label,
                "facility_type": HEALTH_SPEC.facility_type,
            },
            file_size_bytes=0,
            domain=HEALTH_SPEC.domain,
            country_code=components.country_code,
        )
        yield RunRequest(
            run_key=str(path),
            run_config=RunConfig(ops=run_ops),
            tags={
                "country": components.country_code,
                "facility_type": HEALTH_SPEC.facility_type,
            },
        )
        count += 1
    if count == 0:
        yield SkipReason(f"No deletions detected in {source}")
