import io
import json
import typing
from typing import Literal, TypeAlias

import pandas as pd
from azure.storage.filedatalake import DataLakeFileClient, StorageStreamDownloader
from deltalake import DeltaTable, write_deltalake

from src.io_managers.common import get_dir_client
from src.settings import settings

AdlsSupportedFileTypes: TypeAlias = Literal["delta", "csv", "json", "parquet", "excel"]

ADLS_SUPPORTED_FILE_TYPES: tuple[AdlsSupportedFileTypes, ...] = typing.get_args(
    AdlsSupportedFileTypes
)


def _infer_filetype(filename: str, filetype: str) -> str:
    if filetype is None:
        filename_split = filename.split(".")
        if len(filename_split) < 2 and filetype is None:
            raise ValueError(f"Could not infer type from {filename=}")

        if (ext := filename_split[-1].lower()) in ["xls", "xlsx"]:
            return "excel"
        return ext

    return filetype


def adls_loader(
    path_prefix: str,
    filename: str,
    filetype: AdlsSupportedFileTypes = None,
) -> pd.DataFrame:
    filetype = _infer_filetype(filename, filetype)
    dir_client = get_dir_client(path_prefix)
    file_client = dir_client.get_file_client(filename)
    file_stream: StorageStreamDownloader = file_client.download_file()
    with io.BytesIO() as buffer:
        file_stream.readinto(buffer)
        buffer.seek(0)
        match filetype:
            case "delta":
                df = DeltaTable(
                    table_uri=f"abfs://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_BLOB_SAS_HOST}/{path_prefix}/{filename}",
                    storage_options={
                        "AZURE_STORAGE_SAS_TOKEN": settings.AZURE_SAS_TOKEN,
                    },
                )
            case "json":
                data = json.load(buffer)
                if isinstance(data, list):
                    df = pd.json_normalize(data, sep=".")
                else:
                    if "data" in data.keys() and isinstance(data["data"], list):
                        df = pd.json_normalize(data["data"], sep=".")
                    else:
                        raise ValueError(f"Cannot infer schema of JSON file {filename}")
            case _:
                options = {}
                if filetype not in ADLS_SUPPORTED_FILE_TYPES:
                    raise NotImplementedError(f"Unsupported file type `{filetype}`")
                if filetype == "excel":
                    options.update(dict(header=1))
                df = getattr(pd, f"read_{filetype}")(buffer, **options)
    return df


def adls_saver(
    df: pd.DataFrame,
    path_prefix: str,
    filename: str,
    filetype: AdlsSupportedFileTypes = None,
) -> None:
    filetype = _infer_filetype(filename, filetype)
    match filetype:
        case "delta":
            write_deltalake(
                table_or_uri=f"abfs://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_BLOB_SAS_HOST}/{path_prefix}/{filename}",
                storage_options={
                    "AZURE_STORAGE_SAS_TOKEN": settings.AZURE_SAS_TOKEN,
                },
                data=df,
                mode="append",
                configuration={
                    "readChangeFeed": "true",
                    # TODO: This does not actually enable CDF
                    "enableChangeDataFeed": "true",
                    "userMetadata": json.dumps(
                        {
                            "author": "kenneth@thinkingmachin.es",
                        }
                    ),
                },
            )
        case _:
            if filetype not in ADLS_SUPPORTED_FILE_TYPES:
                raise NotImplementedError(f"Unsupported file type `{filetype}`")

            common_args = dict(index=False)
            if filetype == "json":
                common_args.update(dict(indent=2, orient="records"))

            text_data = getattr(df, f"to_{filetype}")(*common_args)
            if not isinstance(text_data, bytes):
                text_data = text_data.encode()

            dir_client = get_dir_client(path_prefix)
            file_client: DataLakeFileClient = dir_client.create_file(filename)
            with io.BytesIO(text_data) as buffer:
                file_client.upload_data(buffer, overwrite=True)
