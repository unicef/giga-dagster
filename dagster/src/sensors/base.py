from abc import ABC
from collections.abc import Mapping

from pydantic import BaseModel, Field

from dagster import Config, SensorEvaluationContext
from src.exceptions import FilenameValidationException
from src.schemas.filename_components import FilenameComponents
from src.utils.adls import deconstruct_filename_components
from src.utils.datahub.builders import build_dataset_urn


class BaseFileConfig(Config, ABC):
    filepath: str = Field(
        description="The path of the file inside the ADLS container relative to the root."
    )
    dataset_type: str = Field(
        description="The type of the dataset, e.g. geolocation, coverage, qos"
    )
    metadata: dict = Field(
        default_factory=dict,
        description="""
        The file metadata including entries from the Ingestion Portal, as well as other system-generated metadata.
        """,
    )
    file_size_bytes: int

    @property
    def filename_components(self) -> FilenameComponents:
        return deconstruct_filename_components(self.filepath)

    @property
    def datahub_dataset_urn(self) -> str:
        return build_dataset_urn(self.filepath)

    def validate_filename(self):
        assert self.filename_components


class AssetFileConfig(BaseFileConfig):
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


class MultiAssetFileConfig(BaseFileConfig):
    destination_filepath: Mapping[str, str] = Field(
        description="""
        The destination path of the file inside the ADLS container relative to the root.

        For multi-assets, pass in a mapping of the output name to the destination path.
        """,
    )
    metastore_schema: Mapping[str, str] = Field(
        description="""
        A mapping of asset keys to names of the Hive Metastore schemas to register the datasets to. Used if the output
        format is a Delta Table. To get the list of valid schemas, run
        ```sql
        SHOW TABLES IN `schemas`
        ```
        or inspect ADLS at the path `giga-dataops-{env}/warehouse/schemas.db`.
        """,
    )


class AssetOpDestinationMapping(BaseModel):
    path: str
    metastore_schema: str


class MultiAssetOpDestinationMapping(BaseModel):
    path: dict[str, str]
    metastore_schema: dict[str, str]


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


def generate_run_ops(
    context: SensorEvaluationContext,
    ops_destination_mapping: dict[
        str, AssetOpDestinationMapping | MultiAssetOpDestinationMapping
    ],
    filepath: str,
    dataset_type: str,
    metadata: dict,
    file_size_bytes: int,
):
    run_ops = {}

    for asset_key, op_mapping in ops_destination_mapping.items():
        common_config = {
            "filepath": filepath,
            "dataset_type": dataset_type,
            "metadata": metadata,
            "file_size_bytes": file_size_bytes,
        }

        if isinstance(op_mapping, MultiAssetOpDestinationMapping):
            file_config = MultiAssetFileConfig(
                **common_config,
                destination_filepath=op_mapping.path,
                metastore_schema=op_mapping.metastore_schema,
            )
        else:
            file_config = AssetFileConfig(
                filepath=filepath,
                dataset_type=dataset_type,
                metadata=metadata,
                file_size_bytes=file_size_bytes,
                destination_filepath=op_mapping.path,
                metastore_schema=op_mapping.metastore_schema,
            )

        try:
            file_config.validate_filename()
        except FilenameValidationException as exc:
            context.log.error(exc)
            continue

        run_ops[asset_key] = file_config

    return run_ops
