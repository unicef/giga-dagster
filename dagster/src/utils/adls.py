import json
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from delta.tables import DeltaTable
from loguru import logger
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StructType

import azure.core.exceptions
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.filedatalake import (
    DataLakeFileClient,
    DataLakeServiceClient,
    FileProperties,
    FileSystemClient,
    PathProperties,
)
from dagster import ConfigurableResource, OpExecutionContext, OutputContext
from src.settings import settings
from src.utils.op_config import FileConfig


class AzuriteBlobPathProperties(NamedTuple):
    """Minimal PathProperties-like object for Azurite compatibility."""

    name: str
    is_directory: bool
    last_modified: object
    content_length: int | None


def _create_datalake_client() -> DataLakeServiceClient:
    """Create DataLakeServiceClient for production Azure."""
    return DataLakeServiceClient(
        account_url=f"https://{settings.AZURE_DFS_SAS_HOST}",
        credential=settings.AZURE_SAS_TOKEN,
    )


def _create_blob_client_for_azurite() -> BlobServiceClient:
    """Create BlobServiceClient for Azurite local development."""
    # Azurite connection string with account key (port 80 for wasb:// compatibility)
    connection_string = (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={settings.AZURE_STORAGE_ACCOUNT_NAME};"
        f"AccountKey={settings.AZURE_STORAGE_ACCOUNT_KEY};"
        f"BlobEndpoint=http://azurite:80/{settings.AZURE_STORAGE_ACCOUNT_NAME};"
    )
    return BlobServiceClient.from_connection_string(connection_string)


def get_blob_service_client() -> BlobServiceClient:
    """Get the correct BlobServiceClient for the current environment."""
    if settings.USE_AZURITE:
        return _create_blob_client_for_azurite()
    return BlobServiceClient(
        account_url=f"https://{settings.AZURE_BLOB_SAS_HOST}",
        credential=settings.AZURE_SAS_TOKEN,
    )


# Lazy initialization of clients
_client: DataLakeServiceClient | None = None
_adls: FileSystemClient | None = None
_blob_client: BlobServiceClient | None = None
_container_client: ContainerClient | None = None


def _get_datalake_client() -> DataLakeServiceClient:
    """Get the DataLake client, initializing lazily."""
    global _client
    if _client is None:
        _client = _create_datalake_client()
    return _client


def _get_adls_filesystem() -> FileSystemClient:
    """Get the ADLS filesystem client, initializing lazily."""
    global _adls
    if _adls is None:
        _adls = _get_datalake_client().get_file_system_client(
            file_system=settings.AZURE_BLOB_CONTAINER_NAME
        )
    return _adls


def _get_blob_client() -> BlobServiceClient:
    """Get the Blob client for Azurite, initializing lazily."""
    global _blob_client
    if _blob_client is None:
        _blob_client = _create_blob_client_for_azurite()
    return _blob_client


def _get_container_client() -> ContainerClient:
    """Get the container client for Azurite, initializing lazily."""
    global _container_client
    if _container_client is None:
        _container_client = _get_blob_client().get_container_client(
            settings.AZURE_BLOB_CONTAINER_NAME
        )
        # Ensure container exists for Azurite
        try:
            _container_client.create_container()
            logger.info(f"Created container: {settings.AZURE_BLOB_CONTAINER_NAME}")
        except azure.core.exceptions.ResourceExistsError:
            pass  # Container already exists
    return _container_client


class ADLSFileClient(ConfigurableResource):
    @staticmethod
    def download_raw(filepath: str) -> bytes:
        if settings.USE_AZURITE:
            blob_client = _get_container_client().get_blob_client(filepath)
            return blob_client.download_blob().readall()
        else:
            file_client = _get_adls_filesystem().get_file_client(filepath)
            with BytesIO() as buffer:
                file_client.download_file().readinto(buffer)
                buffer.seek(0)
                return buffer.read()

    @staticmethod
    def upload_raw(context: OutputContext | None, data: bytes, filepath: str) -> None:
        metadata = context.step_context.op_config["metadata"] if context else None

        if settings.USE_AZURITE:
            blob_client = _get_container_client().get_blob_client(filepath)
            try:
                blob_client.upload_blob(data, overwrite=True, metadata=metadata)
            except azure.core.exceptions.ResourceModifiedError:
                logger.warning("ResourceModifiedError: Skipping write")
            except azure.core.exceptions.ResourceNotFoundError as e:
                logger.error(f"ResourceNotFoundError: {filepath}")
                raise e
        else:
            file_client = _get_adls_filesystem().get_file_client(filepath)
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
        if settings.USE_AZURITE:
            blob_client = _get_container_client().get_blob_client(filepath)
            data = blob_client.download_blob().readall()
            buffer = BytesIO(data)
        else:
            file_client = _get_adls_filesystem().get_file_client(filepath)
            buffer = BytesIO()
            file_client.download_file().readinto(buffer)
            buffer.seek(0)

        ext = Path(filepath).suffix

        if ext == ".csv":
            return pd.read_csv(buffer)

        if ext in [".xls", ".xlsx"]:
            return pd.read_excel(buffer)

        if ext == ".parquet":
            return pd.read_parquet(buffer)

        raise ValueError(f"Unsupported format for file: {filepath}")

    def download_csv_as_spark_dataframe(
        self,
        filepath: str,
        spark: SparkSession,
        schema: StructType = None,
    ) -> sql.DataFrame:
        adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}"
        reader_params = {
            "path": adls_path,
            "header": True,
            "escape": '"',
            "multiLine": True,
        }
        if schema is None:
            df = spark.read.csv(**reader_params)
            # We always want school_id_govt to be treated as a string. Especially important if the data has leading zeroes which will be dropped if it is coalesced to an integer
            if "school_id_govt" in df.columns:
                df = df.withColumn(
                    "school_id_govt", f.col("school_id_govt").cast("string")
                )
                schema = df.schema
                return spark.read.csv(**reader_params, schema=schema)
            else:
                return spark.read.csv(**reader_params)

        return spark.read.csv(**reader_params, schema=schema)

    def download_parquet_as_spark_dataframe(
        self, filepath: str, spark: SparkSession
    ) -> sql.DataFrame:
        adls_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/{filepath}"
        return spark.read.parquet(adls_path)

    def upload_pandas_dataframe_as_file(
        self,
        context: OutputContext | OpExecutionContext,
        data: pd.DataFrame,
        filepath: str,
    ) -> None:
        ext = Path(filepath).suffix
        if not ext:
            raise RuntimeError(f"Cannot infer format of file {filepath}")

        match ext:
            case ".csv" | ".xls" | ".xlsx":
                bytes_data = data.to_csv(index=False).encode("utf-8-sig")
            case ".json":
                bytes_data = data.to_json(index=False, indent=2).encode()
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

        if settings.USE_AZURITE:
            blob_client = _get_container_client().get_blob_client(filepath)
            blob_client.upload_blob(bytes_data, overwrite=True, metadata=metadata)
        else:
            file_client = _get_adls_filesystem().get_file_client(filepath)
            with BytesIO(bytes_data) as buffer:
                buffer.seek(0)
                file_client.upload_data(
                    buffer.read(), overwrite=True, metadata=metadata
                )

    def download_delta_table_as_spark_dataframe(
        self,
        table_name: str,
        spark: SparkSession,
    ) -> sql.DataFrame:
        return DeltaTable.forName(spark, table_name).toDF()

    def download_json(self, filepath: str) -> dict | list:
        if settings.USE_AZURITE:
            blob_client = _get_container_client().get_blob_client(filepath)
            data = blob_client.download_blob().readall()
            return json.loads(data)
        else:
            file_client = _get_adls_filesystem().get_file_client(filepath)
            with BytesIO() as buffer:
                file_client.download_file().readinto(buffer)
                buffer.seek(0)
                return json.load(buffer)

    def upload_json(self, data: dict | list[dict], filepath: str) -> None:
        json_data = json.dumps(data, indent=2).encode()

        if settings.USE_AZURITE:
            blob_client = _get_container_client().get_blob_client(filepath)
            blob_client.upload_blob(json_data, overwrite=True)
        else:
            file_client = _get_adls_filesystem().get_file_client(filepath)
            with BytesIO(json_data) as buffer:
                buffer.seek(0)
                file_client.upload_data(buffer.read(), overwrite=True)

    def list_paths(
        self, path: str, *, recursive=True
    ) -> list[PathProperties] | list[AzuriteBlobPathProperties]:
        if settings.USE_AZURITE:
            # Use Blob API for Azurite (doesn't support Data Lake get_paths)
            container = _get_container_client()
            blobs = container.list_blobs(name_starts_with=path)
            return [
                AzuriteBlobPathProperties(
                    name=blob.name,
                    is_directory=False,  # Blob storage doesn't have real directories
                    last_modified=blob.last_modified,
                    content_length=blob.size,
                )
                for blob in blobs
            ]
        else:
            paths = _get_adls_filesystem().get_paths(path=path, recursive=recursive)
            return list(paths)

    def list_paths_generator(
        self,
        path: str,
        *,
        recursive=True,
    ) -> Iterator[PathProperties] | Iterator[AzuriteBlobPathProperties]:
        if settings.USE_AZURITE:
            container = _get_container_client()
            blobs = container.list_blobs(name_starts_with=path)
            for blob in blobs:
                yield AzuriteBlobPathProperties(
                    name=blob.name,
                    is_directory=False,
                    last_modified=blob.last_modified,
                    content_length=blob.size,
                )
        else:
            return _get_adls_filesystem().get_paths(path=path, recursive=recursive)

    def get_file_metadata(self, filepath: str) -> FileProperties:
        if settings.USE_AZURITE:
            blob_client = _get_container_client().get_blob_client(filepath)
            return blob_client.get_blob_properties()
        else:
            file_client = _get_adls_filesystem().get_file_client(filepath)
            return file_client.get_file_properties()

    def rename_file(self, old_filepath: str, new_filepath: str) -> DataLakeFileClient:
        if settings.USE_AZURITE:
            # Azurite doesn't support rename, so we copy and delete
            container = _get_container_client()
            source_blob = container.get_blob_client(old_filepath)
            dest_blob = container.get_blob_client(new_filepath)

            # Copy the blob
            dest_blob.start_copy_from_url(source_blob.url)
            # Delete the original
            source_blob.delete_blob()
            logger.info(f"File {old_filepath} renamed to {new_filepath}")
            return dest_blob
        else:
            file_client = _get_adls_filesystem().get_file_client(file_path=old_filepath)
            new_path = file_client.file_system_name + "/" + new_filepath
            renamed_file_client = file_client.rename_file(new_name=new_path)
            print(f"File {old_filepath} renamed to {new_path}")
            return renamed_file_client

    def folder_exists(self, folder_path: str) -> bool:
        if settings.USE_AZURITE:
            # Check if any blobs exist with this prefix
            container = _get_container_client()
            blobs = list(
                container.list_blobs(name_starts_with=folder_path, results_per_page=1)
            )
            return len(blobs) > 0
        else:
            file_system_client = _get_datalake_client().get_file_system_client(
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
        blob_service_client = get_blob_service_client()

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
        if settings.USE_AZURITE:
            if is_directory:
                # Delete all blobs with this prefix
                container = _get_container_client()
                blobs = container.list_blobs(name_starts_with=filepath)
                for blob in blobs:
                    container.delete_blob(blob.name)
            else:
                blob_client = _get_container_client().get_blob_client(filepath)
                blob_client.delete_blob()
        else:
            file_client = _get_adls_filesystem().get_file_client(file_path=filepath)
            file_client.delete_file(recursive=is_directory)
