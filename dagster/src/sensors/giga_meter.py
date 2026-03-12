from datetime import UTC, datetime

from dagster import RunConfig, RunRequest, SensorEvaluationContext, SkipReason, sensor
from src.assets.giga_meter.assets import ParquetToDeltaConfig
from src.constants import constants
from src.jobs.giga_meter import giga_meter_connectivity_ping_checks
from src.utils.adls import ADLSFileClient


def _parse_cursor(context: SensorEvaluationContext) -> tuple[float, str]:
    """
    Cursor format: "timestamp|filename"
    Example: "1709459183.123|file_a.parquet"
    """
    if not context.cursor:
        return 0.0, ""

    timestamp_str, filename = context.cursor.split("|", 1)
    return float(timestamp_str), filename


def _build_cursor(timestamp: float, filename: str) -> str:
    return f"{timestamp}|{filename}"


def _get_file_timestamp(path) -> float:
    """
    Ensures timezone-aware timestamp comparison.
    """
    last_modified: datetime = path.last_modified

    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=UTC)

    return last_modified.timestamp()


def _get_sort_key(path) -> tuple[float, str]:
    return _get_file_timestamp(path), path.name


def _collect_parquet_files(
    adls_file_client: ADLSFileClient,
    upload_path: str,
) -> list:
    return [
        path
        for path in adls_file_client.list_paths_generator(
            upload_path,
            recursive=True,
        )
        if not path.is_directory and path.name.endswith(".parquet")
    ]


def _read_retry_files(adls_file_client: ADLSFileClient) -> list[str]:
    """Read failed file paths from the retry file on ADLS."""
    try:
        data = adls_file_client.download_json(constants.PING_RETRY_FILE_PATH)
        if isinstance(data, list) and data:
            return data
    except Exception:
        pass
    return []


@sensor(
    job=giga_meter_connectivity_ping_checks,
    minimum_interval_seconds=43200,  # 12 hours
)
def giga_meter_parquet_to_delta_sensor(
    context: SensorEvaluationContext,
    adls_file_client: ADLSFileClient,
):
    upload_path = constants.PING_PARQUET_PATH

    last_timestamp, last_filename = _parse_cursor(context)

    parquet_files = _collect_parquet_files(
        adls_file_client=adls_file_client,
        upload_path=upload_path,
    )

    # Deterministic ordering
    parquet_files.sort(key=_get_sort_key)

    new_files: list[str] = []
    new_cursor_timestamp = last_timestamp
    new_cursor_filename = last_filename

    for path in parquet_files:
        file_timestamp, file_name = _get_sort_key(path)

        if (file_timestamp, file_name) <= (last_timestamp, last_filename):
            continue

        new_files.append(file_name)

        new_cursor_timestamp = file_timestamp
        new_cursor_filename = file_name

    # Include files that failed in the previous batch
    retry_files = _read_retry_files(adls_file_client)
    if retry_files:
        context.log.info(f"Including {len(retry_files)} retry files from previous run.")

    # Merge: retry files + new files (deduplicate)
    all_files = list(set(dict.fromkeys(retry_files + new_files)))

    if not all_files:
        yield SkipReason("No new or retry parquet files found.")
        return

    # Update cursor only after full evaluation
    if new_cursor_timestamp > last_timestamp:
        context.update_cursor(_build_cursor(new_cursor_timestamp, new_cursor_filename))

    yield RunRequest(
        run_key=f"batch:{new_cursor_timestamp}:{len(all_files)}",
        run_config=RunConfig(
            ops={
                "connectivity_ping_checks": ParquetToDeltaConfig(
                    upload_path=upload_path,
                    files=all_files,
                )
            }
        ),
    )
