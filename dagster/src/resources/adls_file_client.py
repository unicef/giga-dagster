import json
from io import BytesIO

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

from ..settings import AZURE_BLOB_CONTAINER_NAME, AZURE_DFS_SAS_HOST, AZURE_SAS_TOKEN


class ADLSFileClient:
    def __init__(self):
        self.client = DataLakeServiceClient(
            account_url=f"https://{AZURE_DFS_SAS_HOST}", credential=AZURE_SAS_TOKEN
        )
        self.adls = self.client.get_file_system_client(
            file_system=AZURE_BLOB_CONTAINER_NAME
        )

    def download_adls_csv_to_pandas(self, filepath: str):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return pd.read_csv(buffer)

    def upload_pandas_to_adls_csv(self, filepath: str, data: pd.DataFrame):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            data.to_csv(buffer, index=False)
            buffer.seek(0)
            file_client.upload_data(buffer.getvalue(), overwrite=True)

    def download_adls_json_to_json(self, filepath: str):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return json.load(buffer)

    def upload_json_to_adls_json(self, filepath: str, data):
        file_client = self.adls.get_file_client(filepath)
        json_data = json.dumps(data).encode("utf-8")

        with BytesIO(json_data) as buffer:
            buffer.seek(0)
            file_client.upload_data(buffer.getvalue(), overwrite=True)

    def list_paths(self, path: str, recursive=True):
        paths = self.adls.get_paths(path=path, recursive=recursive)
        return list(paths)
