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


@sensor(
    job=giga_meter_connectivity_ping_checks,
    minimum_interval_seconds=3600,
)
def giga_meter_parquet_to_delta(
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

    if not new_files:
        yield SkipReason("No new parquet files found.")
        return

    # Update cursor only after full evaluation
    context.update_cursor(_build_cursor(new_cursor_timestamp, new_cursor_filename))

    yield RunRequest(
        run_key=f"batch:{new_cursor_timestamp}",
        run_config=RunConfig(
            ops={
                "connectivity_ping_checks": ParquetToDeltaConfig(
                    upload_path=upload_path,
                    files=new_files,
                )
            }
        ),
    )
