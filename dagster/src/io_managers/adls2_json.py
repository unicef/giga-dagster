import io

import pandas as pd
from azure.storage.filedatalake import DataLakeFileClient, StorageStreamDownloader

from dagster import ConfigurableIOManager, InputContext, OutputContext

from .common import (
    get_dir_client,
    get_file_path_from_context,
    get_file_path_from_context_upstream,
)


class Adls2JsonIOManager(ConfigurableIOManager):
    path_prefix = "adls-test"

    @staticmethod
    def _type_handler(obj: pd.DataFrame) -> bytes:
        return obj.to_json(index=False, indent=2).encode()

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | None) -> None:
        if obj is None:
            return

        text_obj = self._type_handler(obj)
        filename = f"{get_file_path_from_context(context)}.json"
        dir_client = get_dir_client(self.path_prefix)
        file_client: DataLakeFileClient = dir_client.create_file(filename)
        with io.BytesIO(text_obj) as buffer:
            file_client.upload_data(buffer, overwrite=True)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        filename = get_file_path_from_context_upstream(context)
        dir_client = get_dir_client(self.path_prefix)
        file_client = dir_client.get_file_client(f"{filename}.json")
        file_stream: StorageStreamDownloader = file_client.download_file()
        with io.BytesIO() as buffer:
            file_stream.readinto(buffer)
            buffer.seek(0)
            df = pd.read_json(buffer)
        return df
