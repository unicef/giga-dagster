import pandas as pd

from dagster import InputContext, IOManager, OutputContext
from src.resources.adls_file_client import ADLSFileClient


class StagingADLSIOManager(IOManager):
    def __init__(self):
        self.adls_client = ADLSFileClient()

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        if output.empty:
            context.log.warning("Output DataFrame is empty. Skipping write operation.")
            return

        filepath = self._get_filepath(context)
        self.adls_client.upload_to_adls(filepath, output)

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {('/').join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext):
        filepath = self._get_filepath(context.upstream_output)

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {('/').join(filepath.split('/')[:-1])} in ADLS."
        )

        return self.adls_client.download_from_adls(filepath)

    def _get_filepath(self, context):
        filepath = context.step_context.op_config["filepath"]
        parent_folder = context.step_context.op_config["dataset_type"]

        filename = filepath.split("/")[-1]
        step = context.step_key

        step_destination_folder_map = {
            "raw": "adls-testing-raw",
            "bronze": f"bronze/{parent_folder}",
            "dq_passed_rows": f"staging/pending-review/{parent_folder}",
            "dq_failed_rows": "archive/gx-tests-failed",
            "manual_review_passed_rows": f"staging/approved/{parent_folder}",
            "manual_review_failed_rows": "archive/manual-review-rejected",
            "silver": f"silver/{parent_folder}",
            "gold": "gold",
        }

        destination_folder = step_destination_folder_map[step]

        if not destination_folder:
            raise ValueError(f"Unknown filepath: {filepath}")

        destination_filepath = f"{destination_folder}/{filename}"
        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath
