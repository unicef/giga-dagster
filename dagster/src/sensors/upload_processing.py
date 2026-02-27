from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.assets.upload_processing.parquet_to_delta import ParquetToDeltaConfig
from src.constants import constants
from src.jobs.upload_processing import upload_processing_job
from src.utils.adls import ADLSFileClient


@sensor(
    job=upload_processing_job,
    minimum_interval_seconds=7200,
)
def upload_processing_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    upload_path = constants.PING_PARQUET_PATH

    new_requests = []

    for path in adls_file_client.list_paths_generator(
        upload_path,
        recursive=True,
    ):
        if path.is_directory or not path.name.endswith(".parquet"):
            continue
        context.log.info(f"Processing {path.name}")
        file_path = path.name
        etag = path.etag

        run_key = f"{file_path}:{etag}"

        new_requests.append(
            RunRequest(
                run_key=run_key,
                run_config=RunConfig(
                    ops={
                        "convert_parquets_to_delta": ParquetToDeltaConfig(
                            upload_path=file_path,
                            etag=etag,
                        )
                    }
                ),
            )
        )

    if not new_requests:
        yield SkipReason("No parquet files found.")

    yield from new_requests
