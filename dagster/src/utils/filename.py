from datetime import datetime
from pathlib import Path

from src.exceptions import FileExtensionValidationException, FilenameValidationException
from src.schemas.filename_components import FilenameComponents


def deconstruct_school_master_filename_components(filepath: str):
    """Deconstruct and validate filename components for files uploaded through the Ingestion Portal"""
    path = Path(filepath)
    splits = path.stem.split("_")
    expected_timestamp_format = "%Y%m%d-%H%M%S"

    EXPECTED_UPLOAD_FILENAME_COMPONENTS = 4 if "geolocation" in path.stem else 5
    EXPECTED_APPROVAL_FILENAME_COMPONENTS = 3

    if len(splits) == EXPECTED_UPLOAD_FILENAME_COMPONENTS:
        id, country_code, dataset_type = splits[0:3]
        timestamp = splits[-1]
        source = splits[3] if "coverage" in path.stem else None

        return FilenameComponents(
            id=id,
            dataset_type=dataset_type,
            timestamp=datetime.strptime(timestamp, expected_timestamp_format),
            source=source,
            country_code=country_code,
        )
    elif len(splits) == EXPECTED_APPROVAL_FILENAME_COMPONENTS and path.parts[
        -1
    ].endswith("json"):
        country_code, dataset_type, timestamp = splits
        timestamp = datetime.fromtimestamp(int(timestamp))
        return FilenameComponents(
            id="",
            dataset_type=dataset_type,
            timestamp=timestamp,
            country_code=country_code,
        )
    elif len(
        splits
    ) == EXPECTED_APPROVAL_FILENAME_COMPONENTS and not path.stem.endswith(".json"):
        raise FileExtensionValidationException(
            f"Expected {EXPECTED_APPROVAL_FILENAME_COMPONENTS} components for filename `{path.name}`; got {EXPECTED_APPROVAL_FILENAME_COMPONENTS} and missing `.json` extension"
        )

    else:
        raise ValueError(
            f"Expected {EXPECTED_UPLOAD_FILENAME_COMPONENTS} components for filename `{path.name}`; got {len(splits)}"
        )


def deconstruct_qos_filename_components(filepath: str):
    """Deconstruct and validate filename components for APIs uploaded through the Ingestion Portal"""
    path = Path(filepath)

    if "qos" in path.stem:
        if len(path.parent.name) != 3:
            raise FilenameValidationException(
                f"Expected 3-letter ISO country code for QoS directory; got `{path.parent.name}`",
            )
        return FilenameComponents(
            dataset_type="qos",
            country_code=path.parent.name,
        )


def deconstruct_adhoc_filename_components(filepath: str) -> FilenameComponents | None:
    """Deconstruct and validate filename components for adhoc files"""

    COUNTRY_CODE_LENGTH = 3
    EXPECTED_GEOLOCATION_FILENAME_COMPONENTS = 4
    EXPECTED_COVERAGE_FILENAME_COMPONENTS = 5

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
