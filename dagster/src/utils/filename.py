from datetime import datetime
from pathlib import Path

from src.exceptions import FilenameValidationException
from src.schemas.filename_components import FilenameComponents


def validate_filename(filepath: str):
    path = Path(filepath)
    path_parent = path.parent.name
    splits = path.stem.split("_")

    if len(splits) < 2:
        raise FilenameValidationException(
            f"Expected at least 2 required components for filename `{path.name}`; got {len(splits)}"
        )

    if len(splits[1]) != 3:
        raise FilenameValidationException(
            f"Expected 2nd component of filename to be 3-letter ISO country code; got {splits[1]}"
        )

    if "geolocation" in path_parent and len(splits) != 4:
        raise FilenameValidationException(
            f"Expected 4 components for geolocation filename `{path.name}`; got {len(splits)}"
        )

    if "coverage" in path_parent and len(splits) != 5:
        raise FilenameValidationException(
            f"Expected 5 components for coverage filename `{path.name}`; got {len(splits)}"
        )


def deconstruct_filename_components(filepath: str):
    """Deconstruct filename components for files uploaded through the Ingestion Portal"""

    path = Path(filepath)
    splits = path.stem.split("_")
    expected_timestamp_format = "%Y%m%d-%H%M%S"

    if "school-geolocation" in path.parts:
        validate_filename(filepath)
        id, country_code, dataset_type, timestamp = splits
        return FilenameComponents(
            id=id,
            dataset_type=dataset_type,
            timestamp=datetime.strptime(timestamp, expected_timestamp_format),
            country_code=country_code,
        )

    if "school-coverage" in path.parts:
        validate_filename(filepath)
        id, country_code, dataset_type, source, timestamp = splits
        return FilenameComponents(
            id=id,
            dataset_type=dataset_type,
            timestamp=datetime.strptime(timestamp, expected_timestamp_format),
            source=source,
            country_code=country_code,
        )

    if "qos" in path.parts:
        if len(path.parent.name) != 3:
            raise FilenameValidationException(
                f"Expected 3-letter ISO country code for QoS directory; got `{path.parent.name}`"
            )

        return FilenameComponents(
            dataset_type="qos",
            country_code=path.parent.name,
        )

    return None
