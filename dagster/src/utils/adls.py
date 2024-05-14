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
from azure.storage.filedatalake import (
    DataLakeFileClient,
    DataLakeServiceClient,
    FileProperties,
    PathProperties,
)
from dagster import ConfigurableResource, OutputContext
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
                file_client.upload_data(buffer.read(), metadata=metadata)
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
            return spark.read.csv(**reader_params)

        return spark.read.csv(**reader_params, schema=schema)

    def upload_pandas_dataframe_as_file(
        self,
        context: OutputContext,
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
                bytes_data = data.to_json(index=False, indent=2).encode()
            case ".parquet":
                bytes_data = data.to_parquet(index=False)
            case _:
                raise OSError(f"Unsupported format for file {filepath}")

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

    def download_json(self, filepath: str) -> dict:
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

    @staticmethod
    def delete(filepath: str, *, is_directory=False):
        file_client = _adls.get_file_client(file_path=filepath)
        file_client.delete_file(recursive=is_directory)
