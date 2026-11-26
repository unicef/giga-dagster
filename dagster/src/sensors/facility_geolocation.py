from pathlib import Path

from dagster import (
    RunConfig,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from src.constants import DataTier, constants
from src.facilities import FacilityGeolocationSpec
from src.facility_profiles import load_all_facility_specs
from src.jobs.facility_geolocation import FACILITY_JOBS_BY_SCHEMA_KEY
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.filename import deconstruct_facility_filename_components
from src.utils.op_config import OpDestinationMapping, generate_run_ops


def _staging_table_path(spec: FacilityGeolocationSpec) -> str:
    """Warehouse path of the facility staging table shared by all countries."""
    schema, table = spec.tables.staging_table_full_name.split(".", 1)
    return f"{settings.SPARK_WAREHOUSE_PATH}/{schema}.db/{table}"


def schema_key_from_upload_path(path: Path, upload_directory: str) -> str | None:
    """Extract schema_key from …/<upload_directory>/<ISO3>/<schema_key>/<file>."""
    parts = path.parts
    try:
        idx = parts.index(upload_directory)
    except ValueError:
        return None
    if len(parts) < idx + 4:
        return None
    return parts[idx + 2]


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
            destination_filepath=(f"{dq_root}/dq-summary/{country_code}/{stem}.json"),
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
            destination_filepath=_staging_table_path(spec),
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
    return {
        spec.assets.approved_rows: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{constants.silver_folder}/manual-review/approved/"
                f"{spec.facility_type}/{country_code}/{path.stem}.parquet"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.SILVER,
        ),
        spec.assets.rejected_rows: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{constants.silver_folder}/manual-review/rejected/"
                f"{spec.facility_type}/{country_code}/{path.stem}.parquet"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.SILVER,
        ),
        spec.assets.silver: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{constants.silver_folder}/{spec.tables.silver_schema}/"
                f"{country_code}"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.SILVER,
        ),
        spec.assets.core: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{constants.silver_folder}/{spec.tables.core_schema}/"
                f"{country_code}"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.SILVER,
        ),
        spec.assets.extension: OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath=(
                f"{constants.silver_folder}/{spec.tables.extension_schema}/"
                f"{country_code}"
            ),
            metastore_schema=spec.metastore_schema,
            tier=DataTier.SILVER,
        ),
        f"{prefix}_publish_complete": OpDestinationMapping(
            source_filepath=str(path),
            destination_filepath="",
            metastore_schema=spec.metastore_schema,
            tier=DataTier.SILVER,
        ),
    }


def _build_raw_uploads_sensor(
    *,
    name: str,
    spec: FacilityGeolocationSpec,
    job,
) -> SensorDefinition:
    @sensor(
        name=name,
        job=job,
        minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
    )
    def _raw_uploads_sensor(
        context: SensorEvaluationContext,
        adls_file_client: ADLSFileClient,
    ):
        source = f"{constants.UPLOAD_PATH_PREFIX}/{spec.paths.upload_directory}"
        count = 0
        for file_data in adls_file_client.list_paths_generator(source, recursive=True):
            if file_data.is_directory:
                continue
            path = Path(file_data.name)
            schema_key = schema_key_from_upload_path(path, spec.paths.upload_directory)
            if not spec.paths.matches_schema_key(schema_key):
                continue
            try:
                components = deconstruct_facility_filename_components(str(path))
            except Exception as exc:
                context.log.error(f"Invalid facility filename {path}: {exc}")
                continue
            metadata = adls_file_client.fetch_metadata_for_blob(
                str(path), ensure_exists=True
            )
            properties = adls_file_client.get_file_metadata(filepath=str(path))
            run_ops = generate_run_ops(
                _automated_destinations(spec, path, components.country_code),
                dataset_type=spec.dataset_type,
                metadata={
                    **metadata,
                    "approval_dataset_label": spec.approval_label,
                    "facility_type": spec.facility_type,
                    "schema_key": schema_key,
                },
                file_size_bytes=properties.size,
                domain=spec.domain,
                country_code=components.country_code,
            )
            yield RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={
                    "country": components.country_code,
                    "facility_type": spec.facility_type,
                    "schema_key": schema_key,
                },
            )
            count += 1
        if count == 0:
            yield SkipReason(
                f"No uploads detected in {source} for schema_keys="
                f"{spec.paths.schema_keys}"
            )

    return _raw_uploads_sensor


def _build_post_manual_checks_sensor(
    *,
    name: str,
    spec: FacilityGeolocationSpec,
    job,
) -> SensorDefinition:
    @sensor(
        name=name,
        job=job,
        minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
    )
    def _post_manual_checks_sensor(
        context: SensorEvaluationContext,
        adls_file_client: ADLSFileClient,
    ):
        source = f"{constants.staging_folder}/{spec.paths.approval_directory}"
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
                _post_approval_destinations(spec, path, components.country_code),
                dataset_type=spec.dataset_type,
                metadata={
                    **metadata,
                    "approval_dataset_label": spec.approval_label,
                    "facility_type": spec.facility_type,
                },
                file_size_bytes=0,
                domain=spec.domain,
                country_code=components.country_code,
            )
            yield RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={
                    "country": components.country_code,
                    "facility_type": spec.facility_type,
                },
            )
            count += 1
        if count == 0:
            yield SkipReason(f"No approval files detected in {source}")

    return _post_manual_checks_sensor


def _build_admin_delete_rows_sensor(
    *,
    name: str,
    spec: FacilityGeolocationSpec,
    job,
) -> SensorDefinition:
    @sensor(
        name=name,
        job=job,
        minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
    )
    def _admin_delete_rows_sensor(
        context: SensorEvaluationContext,
        adls_file_client: ADLSFileClient,
    ):
        source = f"{constants.UPLOAD_PATH_PREFIX}/{spec.paths.delete_directory}"
        prefix = spec.assets.raw.removesuffix("_raw")
        delete_asset = f"{prefix}_delete_staging"
        count = 0
        for file_data in adls_file_client.list_paths_generator(source, recursive=True):
            if file_data.is_directory:
                continue
            path = Path(file_data.name)
            try:
                components = deconstruct_facility_filename_components(str(path))
            except Exception as exc:
                context.log.error(f"Invalid delete filename {path}: {exc}")
                continue
            metadata = adls_file_client.fetch_metadata_for_blob(str(path)) or {}
            run_ops = generate_run_ops(
                {
                    delete_asset: OpDestinationMapping(
                        source_filepath=str(path),
                        destination_filepath=_staging_table_path(spec),
                        metastore_schema=spec.metastore_schema,
                        tier=DataTier.STAGING,
                    ),
                },
                dataset_type=spec.dataset_type,
                metadata={
                    **metadata,
                    "approval_dataset_label": spec.approval_label,
                    "facility_type": spec.facility_type,
                },
                file_size_bytes=0,
                domain=spec.domain,
                country_code=components.country_code,
            )
            yield RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={
                    "country": components.country_code,
                    "facility_type": spec.facility_type,
                },
            )
            count += 1
        if count == 0:
            yield SkipReason(f"No deletions detected in {source}")

    return _admin_delete_rows_sensor


# Discovered by load_sensors_from_package_module via this list. Do not also assign
# per-profile SensorDefinition aliases at module level — that duplicates
# registration and can leave None attrs when a profile is absent.
FACILITY_GEOLOCATION_SENSORS: list[SensorDefinition] = []
FACILITY_SENSORS_BY_SCHEMA_KEY: dict[str, dict[str, SensorDefinition]] = {}

for _schema_key, _spec in load_all_facility_specs().items():
    jobs = FACILITY_JOBS_BY_SCHEMA_KEY[_schema_key]
    prefix = f"facility_geolocation_{_schema_key}"
    raw_sensor = _build_raw_uploads_sensor(
        name=f"{prefix}__raw_file_uploads_sensor",
        spec=_spec,
        job=jobs["automated"],
    )
    post_sensor = _build_post_manual_checks_sensor(
        name=f"{prefix}__post_manual_checks_sensor",
        spec=_spec,
        job=jobs["post_approval"],
    )
    delete_sensor = _build_admin_delete_rows_sensor(
        name=f"{prefix}__admin_delete_rows_sensor",
        spec=_spec,
        job=jobs["delete"],
    )
    FACILITY_GEOLOCATION_SENSORS.extend([raw_sensor, post_sensor, delete_sensor])
    FACILITY_SENSORS_BY_SCHEMA_KEY[_schema_key] = {
        "raw": raw_sensor,
        "post_approval": post_sensor,
        "delete": delete_sensor,
    }
