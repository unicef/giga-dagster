from io import BytesIO

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

from dagster import InputContext, IOManager, OutputContext, resource

from ..settings import AZURE_BLOB_CONTAINER_NAME, AZURE_BLOB_SAS_HOST, AZURE_SAS_TOKEN


@resource
class ADLSFileClient:
    def __init__(self):
        self.client = DataLakeServiceClient(
            account_url=f"https://{AZURE_BLOB_SAS_HOST}", credential=AZURE_SAS_TOKEN
        )
        self.adls = self.client.get_file_system_client(
            file_system=AZURE_BLOB_CONTAINER_NAME
        )

    def load_from_adls(self, filepath):
        file_client = self.adls.get_file_client(filepath)
        buffer = BytesIO()
        file_client.download_file().readinto(buffer)
        buffer.seek(0)
        return pd.read_csv(buffer)

    def save_to_adls(self, filepath, data: pd.DataFrame):
        file_client = self.adls.get_file_client(filepath)
        buffer = BytesIO()
        data.to_csv(buffer, index=False)
        buffer.seek(0)
        file_client.upload_data(buffer.getvalue(), overwrite=True)

    def list_paths(self, path, recursive=True):
        paths = self.adls.get_paths(path=path, recursive=recursive)
        return list(paths)


class StagingADLSIOManager(IOManager):
    def __init__(self):
        self.adls_client = ADLSFileClient()

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        if context.step_key == "raw":
            return
        if output.empty:
            context.log.warn("Output DataFrame is empty. Skipping write operation.")
            return

        filepath = self._get_filepath(context)

        self.adls_client.save_to_adls(filepath, output)
        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to {filepath.split('/')[:-1][0]} in"
            " ADLS."
        )

        context.log.info(
            f"opconfig method:{context.step_context.op_config['filepath']}"
        )

    def load_input(self, context: InputContext):
        filepath = self._get_filepath(context.upstream_output)

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from {filepath.split('/')[:-1]} in"
            " ADLS."
        )

        return self.adls_client.load_from_adls(filepath)

    def _get_filepath(self, context):
        filepath = context.step_context.op_config["filepath"]
        filename = filepath.split("/", 1)[-1]
        step = context.step_key

        step_destination_folder_map = {
            "raw": "adls-testing-raw",
            "bronze": "bronze",
            "dq_passed_rows": "staging/pending-review",
            "dq_failed_rows": "archive/gx-tests-failed",
            "manual_review_passed_rows": "staging/approved",
            "manual_review_failed_rows": "archive/manual-review-rejected",
            "silver": "silver",
            "gold": "gold",
        }

        destination_folder = step_destination_folder_map[step]

        if not destination_folder:
            context.log.info(f"Unknown filepath: {filepath}")

        destination_filepath = f"{destination_folder}/{filename}"
        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath
