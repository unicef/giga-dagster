from datetime import datetime
from pathlib import Path

from src.exceptions import FilenameValidationException
from src.schemas.filename_components import FilenameComponents

COUNTRY_CODE_LENGTH = 3
EXPECTED_GEOLOCATION_FILENAME_COMPONENTS = 4
EXPECTED_COVERAGE_FILENAME_COMPONENTS = 5


def deconstruct_filename_components(filepath: str) -> FilenameComponents | None:
    """Deconstruct and validate filename components for files uploaded through the Ingestion Portal"""
    path = Path(filepath)
    splits = path.stem.split("_")
    expected_timestamp_format = "%Y%m%d-%H%M%S"
    parts_except_name = path.parts[:-1]

    if any("geolocation" in p for p in parts_except_name):
        if len(splits) != EXPECTED_GEOLOCATION_FILENAME_COMPONENTS:
            raise FilenameValidationException(
                f"Expected 4 components for geolocation filename `{path.name}`; got {len(splits)}",
            )

        id, country_code, dataset_type, timestamp = splits
        return FilenameComponents(
            id=id,
            dataset_type=dataset_type,
            timestamp=datetime.strptime(timestamp, expected_timestamp_format),
            country_code=country_code,
        )

    if any("coverage" in p for p in parts_except_name):
        if len(splits) != EXPECTED_COVERAGE_FILENAME_COMPONENTS:
            raise FilenameValidationException(
                f"Expected 5 components for coverage filename `{path.name}`; got {len(splits)}",
            )

        id, country_code, dataset_type, source, timestamp = splits
        return FilenameComponents(
            id=id,
            dataset_type=dataset_type,
            timestamp=datetime.strptime(timestamp, expected_timestamp_format),
            source=source,
            country_code=country_code,
        )

    if any("qos" in p for p in parts_except_name):
        if len(path.parent.name) != COUNTRY_CODE_LENGTH:
            raise FilenameValidationException(
                f"Expected 3-letter ISO country code for QoS directory; got `{path.parent.name}`",
            )

        return FilenameComponents(
            dataset_type="qos",
            country_code=path.parent.name,
        )

    if len(country_code := path.stem.split("_")[0]) != COUNTRY_CODE_LENGTH:  # noqa:SIM102
        if len(country_code := path.parent.name) != COUNTRY_CODE_LENGTH:
            return None

    return FilenameComponents(country_code=country_code)
