from datetime import datetime
from pathlib import Path

from src.exceptions import FilenameValidationException
from src.schemas.filename_components import FilenameComponents

EXPECTED_GEOLOCATION_COMPONENTS = 4
EXPECTED_COVERAGE_COMPONENTS = 5
EXPECTED_APPROVED_IDS_COMPONENTS = 3
EXPECTED_DELETE_IDS_COMPONENTS = 2


def deconstruct_school_master_filename_components(filepath: str):
    """Deconstruct and validate filename components for files uploaded through the Ingestion Portal"""
    path = Path(filepath)
    splits = path.stem.split("_")
    expected_timestamp_format = "%Y%m%d-%H%M%S"

    EXPECTED_UPLOAD_FILENAME_COMPONENTS = (
        EXPECTED_GEOLOCATION_COMPONENTS
        if "geolocation" in path.stem
        else EXPECTED_COVERAGE_COMPONENTS
    )

    if len(splits) == EXPECTED_UPLOAD_FILENAME_COMPONENTS:
        id, country_code, dataset_type = splits[0:3]
        timestamp = splits[-1]
        source = splits[3] if "coverage" in path.stem else None

        return FilenameComponents(
            id=id,
            dataset_type=dataset_type,
            timestamp=datetime.strptime(timestamp, expected_timestamp_format),
            country_code=country_code,
            source=source,
        )
    elif len(splits) == EXPECTED_APPROVED_IDS_COMPONENTS and path.parts[-1].endswith(
        ".json"
    ):
        country_code, dataset_type, timestamp = splits
        timestamp = datetime.strptime(timestamp, expected_timestamp_format)
        return FilenameComponents(
            id="",
            dataset_type=dataset_type,
            timestamp=timestamp,
            country_code=country_code,
        )
    elif len(splits) == EXPECTED_DELETE_IDS_COMPONENTS and path.parts[-1].endswith(
        ".json"
    ):
        country_code, timestamp = splits
        timestamp = datetime.strptime(timestamp, expected_timestamp_format)
        return FilenameComponents(
            id="",
            timestamp=timestamp,
            country_code=country_code,
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

    path = Path(filepath)
    parts_except_name = path.parts[:-1]

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


def deconstruct_unstructured_filename_components(
    filepath: str,
) -> FilenameComponents | None:
    """Deconstruct and validate filename components for unstructured files"""

    path = Path(filepath)
    splits = path.stem.split("_")
    expected_timestamp_format = "%Y%m%d-%H%M%S"

    id, country_code, dataset_type = splits[0:3]
    timestamp = splits[-1]

    return FilenameComponents(
        id=id,
        dataset_type=dataset_type,
        timestamp=datetime.strptime(timestamp, expected_timestamp_format),
        country_code=country_code,
    )


if __name__ == "__main__":
    filepath = "raw/uploads/unstructured/PHL/mrm67tmhy5fhuh34q9bc81pr_PHL_unstructured_20240516-090023.png"
    print(deconstruct_unstructured_filename_components(filepath))
