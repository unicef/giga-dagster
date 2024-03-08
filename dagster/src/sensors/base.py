from pydantic import Field

from dagster import Config


class FileConfig(Config):
    filepath: str = Field(
        description="The path of the file inside the ADLS container relative to the root."
    )
    dataset_type: str = Field(description="The type of the dataset.")
    metadata: dict = Field(
        default_factory=dict,
        description="The file metadata including entries from the Ingestion Portal, as well as other system-generated metadata.",
    )
    file_size_bytes: int
    metastore_schema: str = Field(
        description="The name of the Hive Metastore schema to register this dataset to. Used if the output format is a Delta Table."
    )
    unique_identifier_column: str = Field(
        "school_id_giga", description="The name of the primary key column."
    )
    partition_columns: list[str] = Field(
        default_factory=list,
        description="The list of columns to partition the Delta Lake table by.",
    )


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
