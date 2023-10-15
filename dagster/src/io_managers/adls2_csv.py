import io
from typing import Any

import pandas as pd
from azure.storage.filedatalake import (
    DataLakeFileClient,
    DataLakeServiceClient,
    StorageStreamDownloader,
)

from dagster import ConfigurableIOManager, InputContext, OutputContext
from src.settings import settings


class Adls2CsvIOManager(ConfigurableIOManager):
    path_prefix = "raw"

    def __init__(self, **data):
        super().__init__(**data)

    def _get_service_client(self):
        return DataLakeServiceClient(
            f"https://{settings.AZURE_BLOB_SAS_HOST}",
            credential=settings.AZURE_SAS_TOKEN,
        )

    def _get_fs_client(self):
        service_client = self._get_service_client()
        return service_client.get_file_system_client(settings.AZURE_BLOB_CONTAINER_NAME)

    def _get_dir_client(self):
        fs_client = self._get_fs_client()
        dir_client = fs_client.get_directory_client(self.path_prefix)
        if not dir_client.exists():
            dir_client.create_directory()
        return dir_client

    def _get_file_path(self, context):
        file_path = "/".join(context.asset_key.path)
        context.log.info(f"{file_path=}")
        return file_path

    def _type_handler(self, context, obj: Any) -> (bytes, str):
        filename = self._get_file_path(context)
        if isinstance(obj, pd.DataFrame):
            return obj.to_csv(index=False).encode("utf-8-sig"), f"{filename}.csv"
        raise NotImplementedError

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        text_obj, filename = self._type_handler(context, obj)
        file_client: DataLakeFileClient = self._get_dir_client().create_file(filename)
        with io.BytesIO(text_obj) as buffer:
            file_client.upload_data(buffer, overwrite=True)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        filename = self._get_file_path(context)
        file_client = self._get_dir_client().get_file_client(f"{filename}.csv")
        file_stream: StorageStreamDownloader = file_client.download_file()
        with io.BytesIO() as buffer:
            file_stream.readinto(buffer)
        df = pd.read_csv(buffer)
        return df
