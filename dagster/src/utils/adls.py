import json
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path

import pandas as pd
from delta.tables import DeltaTable
from loguru import logger
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import azure.core.exceptions
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import (
    DataLakeFileClient,
    DataLakeServiceClient,
    FileProperties,
    PathProperties,
)
from dagster import ConfigurableResource, OpExecutionContext, OutputContext
from src.settings import settings
from src.utils.op_config import FileConfig

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
    def upload_raw(context: OutputContext, data: bytes, filepath: str) -> None:
        file_client = _adls.get_file_client(filepath)
        metadata = context.step_context.op_config["metadata"]
        with BytesIO(data) as buffer:
            buffer.seek(0)
            try:
                file_client.upload_data(
                    buffer.read(), overwrite=True, metadata=metadata
                )
            except azure.core.exceptions.ResourceModifiedError:
                logger.warning("ResourceModifiedError: Skipping write")
            except azure.core.exceptions.ResourceNotFoundError as e:
                logger.error(f"ResourceNotFoundError: {filepath}")
                raise e

    def download_csv_as_pandas_dataframe(self, filepath: str) -> pd.DataFrame:
        file_client = _adls.get_file_client(filepath)
        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            ext = Path(filepath).suffix

            if ext == ".csv":
                return pd.read_csv(buffer)

            if ext in [".xls", ".xlsx"]:
                return pd.read_excel(buffer)

        raise ValueError(f"Unsupported format for file: {filepath}")

    def download_csv_as_spark_dataframe(
        self,
        filepath: str,
        spark: SparkSession,
        schema: StructType | None = None,
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

    def upload_spark_dataframe_as_files(
        self,
        spark_df: sql.DataFrame,
        directory_path: str,
        file_format: str,
        mode: str = "overwrite",
    ) -> None:
        """
        Write a Spark DataFrame to a directory in ADLS in a distributed manner.

        Args:
            spark_df: The Spark DataFrame to write
            directory_path: The directory path in ADLS (without leading slash)
            file_format: The file format (parquet, csv, json, etc.)
            mode: Write mode (overwrite, append, etc.)
        """
        # Construct the full ADLS path using abfss protocol
        full_adls_path = (
            f"{settings.AZURE_BLOB_CONNECTION_URI}/{directory_path.rstrip('/')}"
        )

        logger.info(
            f"Writing Spark DataFrame to distributed directory: {full_adls_path}"
        )

        writer = spark_df.write.format(file_format).mode(mode)

        # Add format-specific options
        if file_format == "csv":
            writer = writer.option("header", "true").option("escape", '"')
        elif file_format == "json":
            writer = writer.option("multiLine", "true")

        writer.save(full_adls_path)
        logger.info(f"Successfully wrote Spark DataFrame to {full_adls_path}")

    def download_files_as_spark_dataframe(
        self,
        spark: SparkSession,
        directory_path: str,
        file_format: str,
        schema: StructType | None = None,
    ) -> sql.DataFrame:
        """
        Read a directory of part-files (previously written by Spark) into a single Spark DataFrame.

        Args:
            spark: The Spark session
            directory_path: The directory path in ADLS (without leading slash)
            file_format: The file format (parquet, csv, json, etc.)
            schema: Optional schema for the DataFrame

        Returns:
            A Spark DataFrame containing all data from the directory
        """
        # Construct the full ADLS path using abfss protocol
        full_adls_path = (
            f"{settings.AZURE_BLOB_CONNECTION_URI}/{directory_path.rstrip('/')}"
        )

        logger.info(
            f"Reading Spark DataFrame from distributed directory: {full_adls_path}"
        )

        reader = spark.read.format(file_format)

        # Add format-specific options
        if file_format == "csv":
            reader = (
                reader.option("header", "true")
                .option("escape", '"')
                .option("multiLine", "true")
            )
        elif file_format == "json":
            reader = reader.option("multiLine", "true")

        # Apply schema if provided
        if schema is not None:
            reader = reader.schema(schema)

        df = reader.load(full_adls_path)
        logger.info(f"Successfully read Spark DataFrame from {full_adls_path}")
        return df

    def upload_pandas_dataframe_as_file(
        self,
        context: OutputContext | OpExecutionContext,
        data: pd.DataFrame,
        filepath: str,
    ) -> None:
        ext = Path(filepath).suffix
        if not ext:
            raise RuntimeError(f"Cannot infer format of file {filepath}")

        file_client = _adls.get_file_client(filepath)
        match ext:
            case ".csv" | ".xls" | ".xlsx":
                bytes_data = data.to_csv(index=False).encode("utf-8-sig")
            case ".json":
                json_str = data.to_json(index=False, indent=2)
                bytes_data = json_str.encode() if json_str is not None else b""
            case ".parquet":
                bytes_data = data.to_parquet(index=False)
            case _:
                raise OSError(f"Unsupported format for file {filepath}")

        if isinstance(context, OpExecutionContext):
            config = FileConfig(**context._step_execution_context.op_config)

        if isinstance(context, OutputContext):
            config = FileConfig(**context.step_context.op_config)

        metadata = config.metadata
        metadata = {k: v for k, v in metadata.items() if not isinstance(v, dict)}

        with BytesIO(bytes_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.read(), overwrite=True, metadata=metadata)

    def download_delta_table_as_spark_dataframe(
        self,
        table_name: str,
        spark: SparkSession,
    ) -> sql.DataFrame:
        return DeltaTable.forName(spark, table_name).toDF()

    def download_json(self, filepath: str) -> dict | list:
        file_client = _adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return json.load(buffer)

    def upload_json(self, data: dict | list[dict], filepath: str) -> None:
        file_client = _adls.get_file_client(filepath)
        json_data = json.dumps(data, indent=2).encode()

        with BytesIO(json_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.read(), overwrite=True)

    def list_paths(self, path: str, *, recursive=True) -> list[PathProperties]:
        paths = _adls.get_paths(path=path, recursive=recursive)
        return list(paths)

    def list_paths_generator(
        self,
        path: str,
        *,
        recursive=True,
    ) -> Iterator[PathProperties]:
        return _adls.get_paths(path=path, recursive=recursive)

    def get_file_metadata(self, filepath: str) -> FileProperties:
        file_client = _adls.get_file_client(filepath)
        return file_client.get_file_properties()

    def rename_file(self, old_filepath: str, new_filepath: str) -> DataLakeFileClient:
        file_client = _adls.get_file_client(file_path=old_filepath)
        new_path = file_client.file_system_name + "/" + new_filepath
        renamed_file_client = file_client.rename_file(new_name=new_path)
        print(f"File {old_filepath} renamed to {new_path}")
        return renamed_file_client

    def folder_exists(self, folder_path: str) -> bool:
        file_system_client = _client.get_file_system_client(
            file_system=settings.AZURE_BLOB_CONTAINER_NAME
        )

        try:
            file_system_client.get_directory_client(
                folder_path
            ).get_directory_properties()
            return True
        except azure.core.exceptions.ResourceNotFoundError:
            return False

    def copy_folder(self, source_folder: str, target_folder: str) -> None:
        # We use BlobServiceClient to copy folders because DataLakeServiceClient does not support folder copy.
        blob_service_client = BlobServiceClient.from_connection_string(
            settings.AZURE_STORAGE_CONNECTION_STRING
        )
        source_container_client = blob_service_client.get_container_client(
            settings.AZURE_BLOB_CONTAINER_NAME
        )

        source_blobs = source_container_client.list_blobs(
            name_starts_with=source_folder
        )
        for blob in source_blobs:
            if blob.name == source_folder:
                continue

            source_blob_client = source_container_client.get_blob_client(blob.name)

            relative_path = blob.name[len(source_folder) :].lstrip("/")
            destination_blob_name = f"{target_folder.rstrip('/')}/{relative_path}"

            target_blob_client = source_container_client.get_blob_client(
                destination_blob_name
            )

            try:
                target_blob_client.start_copy_from_url(source_blob_client.url)
                print(f"Copied: {blob.name} -> {destination_blob_name}")
            except azure.core.exceptions.ResourceExistsError:
                print(f"Skipping: {blob.name} already exists in {target_folder}")
            except Exception as e:
                print(f"Failed to copy {blob.name}: {e}")

    @staticmethod
    def delete(filepath: str, *, is_directory=False):
        file_client = _adls.get_file_client(file_path=filepath)
        file_client.delete_file(recursive=is_directory)
