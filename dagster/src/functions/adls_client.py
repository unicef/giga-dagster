from azure.storage.filedatalake import DataLakeServiceClient

from ..settings import AZURE_BLOB_CONTAINER_NAME, AZURE_BLOB_SAS_HOST, AZURE_SAS_TOKEN


class ADLSClient:
    def _get_adls_service_client(self):
        return DataLakeServiceClient(
            f"https://{AZURE_BLOB_SAS_HOST}", credential=AZURE_SAS_TOKEN
        )

    def _get_file_system_client(self):
        adls_service_client = self._get_adls_service_client()
        return adls_service_client.get_file_system_client(AZURE_BLOB_CONTAINER_NAME)
