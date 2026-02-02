from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.assets.upload_processing.parquet_to_delta import ParquetToDeltaConfig
from src.constants import constants
from src.jobs.upload_processing import upload_processing_job
from src.settings import settings
from src.utils.adls import ADLSFileClient


@sensor(
    job=upload_processing_job,
    minimum_interval_seconds=settings.DEFAULT_SENSOR_INTERVAL_SECONDS,
)
def upload_processing_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    """
    Directory-based sensor.

    Triggers a single run that scans the upload directory.
    File-level idempotency is handled by the ingestion manifest.
    """
    upload_path = constants.PING_PARQUET_PATH

    # Optional but useful: avoid useless runs when directory is empty
    has_parquet_files = any(
        not path.is_directory and path.name.endswith(".parquet")
        for path in adls_file_client.list_paths_generator(
            upload_path,
            recursive=True,
        )
    )

    if not has_parquet_files:
        yield SkipReason("No parquet files found in upload directory.")
        return

    yield RunRequest(
        run_key=f"upload-processing:{upload_path}",
        run_config=RunConfig(
            ops={
                "convert_parquets_to_delta": ParquetToDeltaConfig(
                    upload_path=upload_path
                )
            }
        ),
    )
