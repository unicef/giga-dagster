import io

import pandas as pd
from azure.storage.filedatalake import StorageStreamDownloader
from deltalake import write_deltalake

from src.io_managers.common import get_dir_client
from src.settings import settings


def load_csv(path_prefix: str, filename: str) -> pd.DataFrame:
    dir_client = get_dir_client(path_prefix)
    file_client = dir_client.get_file_client(filename)
    file_stream: StorageStreamDownloader = file_client.download_file()
    with io.BytesIO() as buffer:
        file_stream.readinto(buffer)
        buffer.seek(0)
        df = pd.read_csv(buffer)
    return df


def save_delta(data: pd.DataFrame, path_prefix: str, filename: str) -> None:
    def get_delta_config():
        return dict(
            table_or_uri=f"abfs://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_BLOB_SAS_HOST}/{path_prefix}/{filename}",
            storage_options={
                "AZURE_STORAGE_SAS_TOKEN": settings.AZURE_SAS_TOKEN,
            },
        )

    write_deltalake(
        **get_delta_config(),
        data=data,
        mode="append",
        configuration={
            # TODO: This does not actually enable CDF
            "enableChangeDataFeed": "true",
        },
    )
