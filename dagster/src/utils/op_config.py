from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from dagster import Config
from src.schemas.filename_components import FilenameComponents
from src.utils.datahub.builders import build_dataset_urn
from src.utils.filename import deconstruct_filename_components


class FileConfig(Config):
    filepath: str = Field(
        description="The path of the file inside the ADLS container relative to the root."
    )
    dataset_type: str = Field(
        description="The type of the dataset, e.g. geolocation, coverage, qos"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="""
        The file metadata including entries from the Ingestion Portal, as well as other system-generated metadata.
        """,
    )
    file_size_bytes: int

    destination_filepath: str = Field(
        description="""
        The destination path of the file inside the ADLS container relative to the root.

        For regular assets, simply pass in the destination path as a string.
        """,
    )
    metastore_schema: str = Field(
        description="""
        The name of the Hive Metastore schema to register this dataset to. Used if the output format is a Delta Table.
        To get the list of valid schemas, run
        ```sql
        SHOW TABLES IN `schemas`
        ```
        or inspect ADLS at the path `giga-dataops-{env}/warehouse/schemas.db`.
        """,
    )

    @property
    def filepath_object(self) -> Path:
        return Path(self.filepath)

    @property
    def destination_filepath_object(self) -> Path:
        return Path(self.destination_filepath)

    @property
    def filename_components(self) -> FilenameComponents:
        return deconstruct_filename_components(self.filepath)

    @property
    def destination_filename_components(self) -> FilenameComponents:
        return deconstruct_filename_components(self.destination_filepath)

    @property
    def datahub_source_dataset_urn(self) -> str:
        return build_dataset_urn(self.filepath)

    @property
    def datahub_destination_dataset_urn(self) -> str:
        return build_dataset_urn(self.destination_filepath)


class OpDestinationMapping(BaseModel):
    source_filepath: str
    destination_filepath: str
    metastore_schema: str


def generate_run_ops(
    ops_destination_mapping: dict[str, OpDestinationMapping],
    dataset_type: str,
    metadata: dict,
    file_size_bytes: int,
):
    run_ops = {}

    for asset_key, op_mapping in ops_destination_mapping.items():
        file_config = FileConfig(
            filepath=op_mapping.source_filepath,
            destination_filepath=op_mapping.destination_filepath,
            metastore_schema=op_mapping.metastore_schema,
            dataset_type=dataset_type,
            metadata=metadata,
            file_size_bytes=file_size_bytes,
        )
        run_ops[asset_key] = file_config

    return run_ops