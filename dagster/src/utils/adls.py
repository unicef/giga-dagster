import json
import os.path
from collections.abc import Iterator
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
from delta.tables import DeltaTable
from loguru import logger
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import azure.core.exceptions
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    FileProperties,
    PathProperties,
)
from dagster import ConfigurableResource, OpExecutionContext, OutputContext
from src.constants import constants
from src.exceptions import FilenameValidationException
from src.schemas.filename_components import FilenameComponents
from src.settings import settings
from src.utils.schema import get_primary_key, get_schema_columns

_client = DataLakeServiceClient(
    account_url=f"https://{settings.AZURE_DFS_SAS_HOST}",
    credential=settings.AZURE_SAS_TOKEN,
)
_adls = _client.get_file_system_client(file_system=settings.AZURE_BLOB_CONTAINER_NAME)


class ADLSFileClient(ConfigurableResource):
    @staticmethod
    def download_raw(filepath: str) -> bytes:
        file_client = _adls.get_file_client(filepath)
        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return buffer.read()

    @staticmethod
    def upload_raw(context: OutputContext, data: bytes, filepath: str):
        file_client = _adls.get_file_client(filepath)
        metadata = context.step_context.op_config["metadata"]
        with BytesIO(data) as buffer:
            buffer.seek(0)
            try:
                file_client.upload_data(buffer.read(), metadata=metadata)
            except azure.core.exceptions.ResourceModifiedError:
                logger.warning("ResourceModifiedError: Skipping write")
                pass
            except azure.core.exceptions.ResourceNotFoundError as e:
                logger.error(f"ResourceNotFoundError: {filepath}")
                raise e

    def download_csv_as_pandas_dataframe(self, filepath: str) -> pd.DataFrame:
        file_client = _adls.get_file_client(filepath)
        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            ext = os.path.splitext(filepath)[1]

            if ext == ".csv":
                return pd.read_csv(buffer)
            elif ext in [".xls", ".xlsx"]:
                return pd.read_excel(buffer)
            else:
                raise ValueError(f"Unsupported format for file: {filepath}")

    def download_csv_as_spark_dataframe(
        self, filepath: str, spark: SparkSession, schema: StructType = None
    ) -> sql.DataFrame:
        adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}"
        reader_params = {
            "path": adls_path,
            "header": True,
            "escape": '"',
            "multiLine": True,
        }
        if schema is None:
            return spark.read.csv(**reader_params)

        return spark.read.csv(**reader_params, schema=schema)

    def upload_pandas_dataframe_as_file(
        self, context: OutputContext, data: pd.DataFrame, filepath: str
    ):
        if len(splits := filepath.split(".")) < 2:
            raise RuntimeError(f"Cannot infer format of file {filepath}")

        file_client = _adls.get_file_client(filepath)
        match splits[-1]:
            case "csv" | "xls" | "xlsx":
                bytes_data = data.to_csv(mode="w+", index=False).encode("utf-8-sig")
            case "json":
                bytes_data = data.to_json(indent=2).encode()
            case _:
                raise OSError(f"Unsupported format for file {filepath}")

        metadata = context.step_context.op_config["metadata"]
        with BytesIO(bytes_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.read(), overwrite=True, metadata=metadata)

    def upload_spark_dataframe_as_file(
        self, data: sql.DataFrame, filepath: str, spark: SparkSession
    ):
        if not (extension := os.path.splitext(filepath)[1]):
            raise RuntimeError(f"Cannot infer format of file {filepath}")

        full_remote_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}"

        match extension:
            case ".csv":
                data.write.csv(
                    full_remote_path,
                    mode="overwrite",
                    quote='"',
                    escapeQuotes=True,
                    header=True,
                )
            case ".json":
                data.write.json(full_remote_path, mode="overwrite")
            case _:
                raise OSError(f"Unsupported format for file {filepath}")

    def download_delta_table_as_delta_table(
        self, table_path: str, spark: SparkSession
    ) -> DeltaTable:
        return DeltaTable.forPath(spark, f"{table_path}")

    def download_delta_table_as_spark_dataframe(
        self, table_path: str, spark: SparkSession
    ) -> sql.DataFrame:
        df = spark.read.format("delta").load(table_path)
        df.show()
        return df

    def upload_spark_dataframe_as_delta_table(
        self,
        data: sql.DataFrame,
        table_path: str,
        schema_name: str,
        spark: SparkSession,
    ):
        table_name = table_path.split("/")[-1]
        full_table_name = f"{schema_name}.{table_name}"
        print(f"tablename: {table_name}, table path: {table_path}")

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        columns = get_schema_columns(spark, schema_name)
        primary_key = get_primary_key(spark, schema_name)
        (
            DeltaTable.createIfNotExists(spark)
            .tableName(full_table_name)
            .addColumns(columns)
            .property("delta.enableChangeDataFeed", "true")
            .execute()
        )

        # TODO: Apply logic for preventing unnecessary updates
        # `master` and `updates` tables must both have a column containing the row hash
        (
            DeltaTable.forName(spark, full_table_name)
            .alias("master")
            .merge(
                data.alias("updates"), f"master.{primary_key} = updates.{primary_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def download_json(self, filepath: str):
        file_client = _adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return json.load(buffer)

    def upload_json(self, data: dict | list[dict], filepath: str):
        file_client = _adls.get_file_client(filepath)
        json_data = json.dumps(data, indent=2).encode()

        with BytesIO(json_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.read(), overwrite=True)

    def list_paths(self, path: str, recursive=True) -> list[PathProperties]:
        paths = _adls.get_paths(path=path, recursive=recursive)
        return list(paths)

    def list_paths_generator(
        self, path: str, recursive=True
    ) -> Iterator[PathProperties]:
        return _adls.get_paths(path=path, recursive=recursive)

    def get_file_metadata(self, filepath: str) -> FileProperties:
        file_client = _adls.get_file_client(filepath)
        properties = file_client.get_file_properties()
        return properties

    def rename_file(self, old_filepath: str, new_filepath: str):
        file_client = _adls.get_file_client(file_path=old_filepath)
        new_path = file_client.file_system_name + "/" + new_filepath
        renamed_file_client = file_client.rename_file(new_name=new_path)
        print(f"File {old_filepath} renamed to {new_path}")
        return renamed_file_client


def get_filepath(source_path: str, dataset_type: str, step: str):
    if "dq_summary" in step:
        filename = source_path.split("/")[-1].replace(".csv", ".json")
    elif step in [
        # "geolocation_dq_passed_rows",
        # "geolocation_dq_failed_rows",
        "geolocation_staging",
        # "coverage_dq_passed_rows",
        # "coverage_dq_failed_rows",
        "coverage_staging",
        # "manual_review_passed_rows",
        # "manual_review_failed_rows",
        "silver",
        "gold",
    ]:
        filename = source_path.split("/")[-1].split("_")[1]
    else:
        filename = source_path.split("/")[-1]

    destination_folder = constants.step_folder_map(dataset_type)[step]

    if not destination_folder:
        raise ValueError(f"Unknown filepath: {source_path}")

    destination_filepath = f"{destination_folder}/{filename}"

    return destination_filepath


def get_output_filepath(context: OpExecutionContext, output_name: str = None):
    if output_name:
        step = output_name
    else:
        step = context.asset_key.to_user_string()

    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    source_path = context.get_step_execution_context().op_config["filepath"]

    destination_filepath = get_filepath(source_path, dataset_type, step)

    return destination_filepath


def get_input_filepath(context: OpExecutionContext) -> str:
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    source_path = context.get_step_execution_context().op_config["filepath"]
    step = context.asset_key.to_user_string()
    origin_step = constants.step_origin_map[step]

    source_filepath = get_filepath(source_path, dataset_type, origin_step)

    return source_filepath


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

    validate_filename(filepath)
    path = Path(filepath)
    path_parent = path.parent.name
    splits = path.stem.split("_")
    expected_timestamp_format = "%Y%m%d-%H%M%S"

    if "geolocation" in path_parent:
        id, country_code, dataset_type, timestamp = splits
        return FilenameComponents(
            id=id,
            dataset_type=dataset_type,
            timestamp=datetime.strptime(timestamp, expected_timestamp_format),
            country_code=country_code,
        )

    if "coverage" in path_parent:
        id, country_code, dataset_type, source, timestamp = splits
        return FilenameComponents(
            id=id,
            dataset_type=dataset_type,
            timestamp=datetime.strptime(timestamp, expected_timestamp_format),
            source=source,
            country_code=country_code,
        )

    id, country_code, *rest = splits
    return FilenameComponents(
        id=id,
        dataset_type="unstructured",
        country_code=country_code,
        rest="_".join(rest),
    )
