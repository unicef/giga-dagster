import pandas as pd

from dagster import InputContext, IOManager, OutputContext
from src.resources.adls_file_client import ADLSFileClient
from src.resources.get_destination_file_path import get_destination_filepath


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
        step = context.step_key

        destination_filepath = get_destination_filepath(filepath, parent_folder, step)

        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath
