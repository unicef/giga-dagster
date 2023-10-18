import io

import pandas as pd
from azure.storage.filedatalake import DataLakeFileClient, StorageStreamDownloader

from dagster import ConfigurableIOManager, InputContext, OutputContext

from .common import get_dir_client, get_file_path_from_context


class Adls2CsvIOManager(ConfigurableIOManager):
    path_prefix = "adls-test"

    @staticmethod
    def _type_handler(obj: pd.DataFrame) -> bytes:
        return obj.to_csv(index=False).encode("utf-8-sig")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        text_obj = self._type_handler(obj)
        filename = f"{get_file_path_from_context(context)}.csv"
        dir_client = get_dir_client(self.path_prefix)
        file_client: DataLakeFileClient = dir_client.create_file(filename)
        with io.BytesIO(text_obj) as buffer:
            file_client.upload_data(buffer, overwrite=True)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        filename = get_file_path_from_context(context)
        dir_client = get_dir_client(self.path_prefix)
        file_client = dir_client.get_file_client(f"{filename}.csv")
        file_stream: StorageStreamDownloader = file_client.download_file()
        with io.BytesIO() as buffer:
            file_stream.readinto(buffer)
        df = pd.read_csv(buffer)
        return df
