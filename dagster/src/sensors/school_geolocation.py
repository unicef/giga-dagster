from pathlib import Path

from models.approval_requests import ApprovalRequest
from sqlalchemy import select, update

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.constants import DataTier, constants
from src.jobs.school_master import (
    school_master_geolocation__admin_delete_rows_job,
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__post_manual_checks_job,
)
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context
from src.utils.filename import deconstruct_school_master_filename_components
from src.utils.op_config import OpDestinationMapping, generate_run_ops

DATASET_TYPE = "geolocation"
DOMAIN = "school"
DOMAIN_DATASET_TYPE = f"{DOMAIN}-{DATASET_TYPE}"
METASTORE_SCHEMA = f"{DOMAIN}_{DATASET_TYPE}"


@sensor(
    job=school_master_geolocation__automated_data_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__raw_file_uploads_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = f"{constants.UPLOAD_PATH_PREFIX}/{DOMAIN_DATASET_TYPE}"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        stem = path.stem
        try:
            filename_components = deconstruct_school_master_filename_components(
                adls_filepath
            )
        except Exception as e:
            context.log.error(f"Failed to deconstruct filename: {adls_filepath}: {e}")
            continue
        else:
            country_code = filename_components.country_code
            metadata = adls_file_client.fetch_metadata_for_blob(
                adls_filepath, ensure_exists=True
            )
            properties = adls_file_client.get_file_metadata(filepath=adls_filepath)
            size = properties.size

            ops_destination_mapping = {
                "geolocation_raw": OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=str(path),
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.RAW,
                ),
                "geolocation_metadata": OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/pipeline_tables.db/school_geolocation_metadata",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.BRONZE,
                ),
                "geolocation_bronze": OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=f"{constants.bronze_folder}/{DOMAIN_DATASET_TYPE}/{country_code}/{stem}.parquet",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.BRONZE,
                ),
                "geolocation_data_quality_results": OpDestinationMapping(
                    source_filepath=f"{constants.bronze_folder}/{DOMAIN_DATASET_TYPE}/{country_code}/{stem}.parquet",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-overall/{country_code}/{stem}.parquet",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "geolocation_data_quality_results_human_readable": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-overall/{country_code}/{stem}.parquet",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-human-readable/{country_code}/{stem}.parquet",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "geolocation_dq_schools_passed_human_readable": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-human-readable/{country_code}/{stem}.parquet",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-passed-rows-human-readable/{country_code}/{stem}.csv",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "geolocation_dq_schools_failed_human_readable": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-human-readable/{country_code}/{stem}.parquet",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-failed-rows-human-readable/{country_code}/{stem}.csv",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "geolocation_data_quality_results_summary": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-summary/{country_code}/{stem}.json",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "geolocation_data_quality_report": OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-report/{country_code}/{stem}.txt",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "geolocation_dq_passed_rows": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-passed-rows/{country_code}/{stem}.csv",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "geolocation_dq_failed_rows": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-overall/{country_code}/{stem}.csv",
                    destination_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-failed-rows/{country_code}/{stem}.csv",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.DATA_QUALITY_CHECKS,
                ),
                "geolocation_staging": OpDestinationMapping(
                    source_filepath=f"{constants.dq_results_folder}/{DOMAIN_DATASET_TYPE}/dq-passed-rows/{country_code}/{stem}.csv",
                    destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_staging.db/{country_code.lower()}",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.STAGING,
                ),
            }

            run_ops = generate_run_ops(
                ops_destination_mapping,
                dataset_type=DATASET_TYPE,
                metadata=metadata,
                file_size_bytes=size,
                domain=DOMAIN,
                dq_target_filepath=f"{constants.bronze_folder}/{DOMAIN_DATASET_TYPE}/{country_code}/{stem}.csv",
                country_code=country_code,
            )

            context.log.info(f"FILE: {path}")
            yield RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            )
            count += 1

    if count == 0:
        yield SkipReason(f"No uploads detected in {source_directory}")


@sensor(
    job=school_master_geolocation__post_manual_checks_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__approval_sensor(
    context: SensorEvaluationContext,
):
    """
    Polls the ApprovalRequest table for geolocation merge requests.
    Triggers the post-manual-checks job when merge_requested=True and
    is_merge_processing=False, then immediately marks is_merge_processing=True
    to prevent double-triggering.
    """
    count = 0

    with get_db_context() as db:
        pending = db.scalars(
            select(ApprovalRequest).where(
                (ApprovalRequest.dataset == f"School {DATASET_TYPE.capitalize()}")
                & ApprovalRequest.merge_requested
                & ~ApprovalRequest.is_merge_processing
            )
        ).all()

        for ar in pending:
            country_code = ar.country
            staging_path = f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_staging.db/{country_code.lower()}"
            silver_path = f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_silver.db/{country_code.lower()}"
            master_path = f"{settings.SPARK_WAREHOUSE_PATH}/school_master.db/{country_code.upper()}"
            reference_path = f"{settings.SPARK_WAREHOUSE_PATH}/school_reference.db/{country_code.upper()}"
            rejected_path = f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_rejected.db/{country_code.lower()}"

            ops_destination_mapping = {
                "manual_review_passed_rows": OpDestinationMapping(
                    source_filepath=staging_path,
                    destination_filepath=silver_path,
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.RAW,
                ),
                "manual_review_failed_rows": OpDestinationMapping(
                    source_filepath=staging_path,
                    destination_filepath=rejected_path,
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.MANUAL_REJECTED,
                ),
                "silver": OpDestinationMapping(
                    source_filepath=staging_path,
                    destination_filepath=silver_path,
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.SILVER,
                ),
                "reset_staging_table": OpDestinationMapping(
                    source_filepath=staging_path,
                    destination_filepath=staging_path,
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.STAGING,
                ),
                "master": OpDestinationMapping(
                    source_filepath=silver_path,
                    destination_filepath=master_path,
                    metastore_schema="school_master",
                    tier=DataTier.GOLD,
                ),
                "reference": OpDestinationMapping(
                    source_filepath=silver_path,
                    destination_filepath=reference_path,
                    metastore_schema="school_reference",
                    tier=DataTier.GOLD,
                ),
                "broadcast_master_release_notes": OpDestinationMapping(
                    source_filepath=master_path,
                    destination_filepath=master_path,
                    metastore_schema="school_master",
                    tier=DataTier.GOLD,
                ),
            }

            run_ops = generate_run_ops(
                ops_destination_mapping,
                dataset_type=DATASET_TYPE,
                metadata={},
                file_size_bytes=0,
                domain=DOMAIN,
                country_code=country_code,
            )

            # Unique run_key per merge request; merge_requested_at is set by the portal
            merge_ts = (
                ar.merge_requested_at.isoformat()
                if ar.merge_requested_at
                else str(ar.id)
            )
            run_key = f"{country_code}:{DATASET_TYPE}:merge:{merge_ts}"

            # Mark as processing immediately to prevent re-triggering on the next tick
            with db.begin():
                db.execute(
                    update(ApprovalRequest)
                    .where(ApprovalRequest.id == ar.id)
                    .values({ApprovalRequest.is_merge_processing: True})
                )

            context.log.info(
                f"Triggering merge for {country_code} (run_key={run_key})"
            )
            yield RunRequest(
                run_key=run_key,
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            )
            count += 1

    if count == 0:
        yield SkipReason("No pending merge requests in ApprovalRequest table")


@sensor(
    job=school_master_geolocation__admin_delete_rows_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def school_master_geolocation__admin_delete_rows_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    count = 0
    source_directory = f"{constants.staging_folder}/delete-row-ids"

    for file_data in adls_file_client.list_paths_generator(
        source_directory, recursive=True
    ):
        if file_data.is_directory:
            continue

        adls_filepath = file_data.name
        path = Path(adls_filepath)
        try:
            filename_components = deconstruct_school_master_filename_components(
                adls_filepath
            )
        except Exception as e:
            context.log.error(f"Failed to deconstruct filename: {adls_filepath}: {e}")
            continue
        else:
            country_code = filename_components.country_code
            metadata = adls_file_client.fetch_metadata_for_blob(adls_filepath)

            ops_destination_mapping = {
                "geolocation_delete_staging": OpDestinationMapping(
                    source_filepath=str(path),
                    destination_filepath=f"{settings.SPARK_WAREHOUSE_PATH}/school_geolocation_staging.db/{country_code.lower()}",
                    metastore_schema=METASTORE_SCHEMA,
                    tier=DataTier.RAW,
                ),
            }

            run_ops = generate_run_ops(
                ops_destination_mapping,
                dataset_type=DATASET_TYPE,
                metadata=metadata,
                file_size_bytes=0,
                domain=DOMAIN,
                country_code=country_code,
            )

            context.log.info(f"FILE: {path}")
            yield RunRequest(
                run_key=str(path),
                run_config=RunConfig(ops=run_ops),
                tags={"country": country_code},
            )
            count += 1

    if count == 0:
        yield SkipReason(f"No files detected in {source_directory}")
