from azure.storage.filedatalake import DataLakeServiceClient

from dagster import InputContext
from src.settings import settings


def get_file_path_from_context(context):
    file_path = "/".join(context.asset_key.path)
    context.log.info(f"{file_path=}")
    return file_path


def get_file_path_from_context_upstream(context: InputContext):
    file_path = "/".join(context.upstream_output.asset_key.path)
    context.log.info(f"{file_path=}")
    return file_path


def get_service_client():
    return DataLakeServiceClient(
        f"https://{settings.AZURE_BLOB_SAS_HOST}",
        credential=settings.AZURE_SAS_TOKEN,
    )


def get_fs_client():
    service_client = get_service_client()
    return service_client.get_file_system_client(settings.AZURE_BLOB_CONTAINER_NAME)


def get_dir_client(path_prefix: str):
    fs_client = get_fs_client()
    dir_client = fs_client.get_directory_client(path_prefix)
    if not dir_client.exists():
        dir_client.create_directory()
    return dir_client
