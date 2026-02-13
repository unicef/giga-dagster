import json
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path
from typing import Optional

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
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import (
    DataLakeFileClient,
    DataLakeServiceClient,
    FileProperties,
    PathProperties,
)
from dagster import ConfigurableResource, OpExecutionContext, OutputContext
from src.constants.constants_class import constants
from src.settings import settings
from src.utils.op_config import FileConfig

_client = DataLakeServiceClient(
    account_url=f"https://{settings.AZURE_DFS_SAS_HOST}",
    credential=settings.AZURE_SAS_TOKEN,
)
_adls = _client.get_file_system_client(file_system=settings.AZURE_BLOB_CONTAINER_NAME)


class ADLSFileClient(ConfigurableResource):
    @staticmethod
    def _get_metadata_path(filepath: str) -> Optional[str]:
        # Normalize paths by stripping leading slashes for comparison
        normalized_filepath = filepath.lstrip("/")
        normalized_prefix = constants.UPLOAD_PATH_PREFIX.lstrip("/")
        normalized_metadata_prefix = constants.UPLOAD_METADATA_PATH_PREFIX.lstrip("/")

        if normalized_filepath.startswith(normalized_prefix):
            return (
                normalized_filepath.replace(
                    normalized_prefix, normalized_metadata_prefix, 1
                )
                + ".metadata.json"
            )

        return None

    @staticmethod
    def download_raw(filepath: str) -> bytes:
        file_client = _adls.get_file_client(filepath)
        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return buffer.read()

    @staticmethod
    def upload_raw(context: OutputContext | None, data: bytes, filepath: str) -> None:
        file_client = _adls.get_file_client(filepath)

        metadata = context.step_context.op_config["metadata"] if context else None

        # 1. Upload the actual file WITHOUT metadata
        with BytesIO(data) as buffer:
            buffer.seek(0)
            try:
                file_client.upload_data(buffer.read(), overwrite=True)
            except azure.core.exceptions.ResourceModifiedError:
                logger.warning("ResourceModifiedError: Skipping write")
            except azure.core.exceptions.ResourceNotFoundError as e:
                logger.error(f"ResourceNotFoundError: {filepath}")
                raise e

        # 2. Create sidecar metadata path
        metadata_blob_path = ADLSFileClient._get_metadata_path(filepath)

        # 3. Upload metadata JSON sidecar
        if metadata_blob_path:
            json_bytes = json.dumps(metadata, indent=2).encode()
            file_client_sidecar = _adls.get_file_client(metadata_blob_path)
            with BytesIO(json_bytes) as buffer:
                buffer.seek(0)
                file_client_sidecar.upload_data(buffer.read(), overwrite=True)

            logger.info(f"Successfully uploaded metadata file to {metadata_blob_path}")

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

    def upload_pandas_dataframe_as_file(
        self,
        context: OutputContext | OpExecutionContext,
        data: pd.DataFrame,
        filepath: str,
    ) -> None:
        bytes_data, config = None, None
        ext = Path(filepath).suffix
        if not ext:
            raise RuntimeError(f"Cannot infer format of file {filepath}")

        file_client = _adls.get_file_client(filepath)

        # convert DF to bytes
        match ext:
            case ".csv" | ".xls" | ".xlsx":
                bytes_data = data.to_csv(index=False).encode("utf-8-sig")
            case ".json":
                bytes_data = data.to_json(index=False, indent=2).encode()
            case ".parquet":
                bytes_data = data.to_parquet(index=False)
            case _:
                raise OSError(f"Unsupported format for file {filepath}")

        # extract metadata object from op_config
        if isinstance(context, OpExecutionContext):
            config = FileConfig(**context._step_execution_context.op_config)
        if isinstance(context, OutputContext):
            config = FileConfig(**context.step_context.op_config)

        metadata = config.metadata
        metadata = {k: v for k, v in metadata.items() if not isinstance(v, dict)}

        # 1. upload file without metadata
        with BytesIO(bytes_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.read(), overwrite=True)

        # 2. create metadata sidecar file
        metadata_blob_path = self._get_metadata_path(filepath)

        if metadata_blob_path:
            metadata_client = _adls.get_file_client(metadata_blob_path)
            json_bytes = json.dumps(metadata, indent=2).encode()
            with BytesIO(json_bytes) as buffer:
                buffer.seek(0)
                metadata_client.upload_data(buffer.read(), overwrite=True)

            logger.info(f"Successfully uploaded metadata file to {metadata_blob_path}")
        return None

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

    def fetch_metadata_for_blob(self, filepath: str):
        """
        Prefer <file>.metadata.json stored next to the file.
        Fallback to ADLS blob properties (legacy).
        Returns a dict or None.
        """
        metadata_blob_path = self._get_metadata_path(filepath)

        # 1. Try JSON sidecar first
        try:
            if metadata_blob_path:
                data = self.download_json(metadata_blob_path)
                if data is not None:
                    logger.debug(f"Found metadata sidecar at: {metadata_blob_path}")
                    return data
        except Exception as e:
            logger.debug(
                f"No metadata sidecar found at: {metadata_blob_path} ({e}), falling back to blob properties"
            )

        # 2. Fallback to ADLS properties
        try:
            properties = self.get_file_metadata(filepath)
            # properties.metadata is dict-like
            if getattr(properties, "metadata", None):
                return dict(properties.metadata)
        except Exception as exc:
            logger.debug(f"Failed to fetch blob metadata for {filepath}: {exc}")

        return None

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

    def exists(self, filepath: str) -> bool:
        try:
            client = _adls.get_file_client(filepath).exists()
            return client.exists()
        except Exception:
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
