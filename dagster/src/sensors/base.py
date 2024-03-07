from pydantic import Field

from dagster import Config


class FileConfig(Config):
    filepath: str
    dataset_type: str
    metadata: dict = Field(default_factory=dict)
    file_size_bytes: int
    metastore_schema: str
    unique_identifier_column: str = Field("school_id_giga")
    partition_columns: list[str] = Field(default_factory=list)


def get_dataset_type(filepath: str) -> str | None:
    if "geolocation" in filepath:
        return "geolocation"
    elif "coverage" in filepath:
        return "coverage"
    elif "reference" in filepath:
        return "reference"
    elif "master" in filepath:
        return "master"
    elif "qos" in filepath:
        return "qos"
    else:
        return None
