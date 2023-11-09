import pandas as pd
from dagster.src.resources.adls_file_client import ADLSFileClient

from dagster import InputContext, IOManager, OutputContext


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
            raise ValueError(f"Unknown filepath: {filepath}")

        destination_filepath = f"{destination_folder}/{filename}"
        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath
