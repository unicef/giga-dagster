from io import BytesIO

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

from ..settings import AZURE_BLOB_CONTAINER_NAME, AZURE_BLOB_SAS_HOST, AZURE_SAS_TOKEN


class ADLSFileClient:
    def __init__(self):
        self.client = DataLakeServiceClient(
            account_url=f"https://{AZURE_BLOB_SAS_HOST}", credential=AZURE_SAS_TOKEN
        )
        self.adls = self.client.get_file_system_client(
            file_system=AZURE_BLOB_CONTAINER_NAME
        )

    def download_from_adls(self, filepath: str):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            file_client.download_file().readinto(buffer)
            buffer.seek(0)
            return pd.read_csv(buffer)

    def upload_to_adls(self, context, filepath: str, data: pd.DataFrame):
        file_client = self.adls.get_file_client(filepath)

        with BytesIO() as buffer:
            metadata = context.step_context.op_config["metadata"]
            data.to_csv(buffer, index=False)
            buffer.seek(0)
            file_client.upload_data(
                buffer.getvalue(), overwrite=True, metadata=metadata
            )

    def list_paths(self, path: str, recursive=True):
        paths = self.adls.get_paths(path=path, recursive=recursive)
        return list(paths)

    def get_file_metadata(self, filepath: str):
        file_client = self.adls.get_file_client(filepath)
        properties = file_client.get_file_properties()
        return properties
