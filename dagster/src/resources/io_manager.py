import pandas as pd
from dagster import InputContext, IOManager, OutputContext
from src._utils.adls import ADLSFileClient, _get_filepath


class StagingADLSIOManager(IOManager):
    def __init__(self):
        self.adls_client = ADLSFileClient()

    def handle_output(self, context: OutputContext, output: pd.DataFrame):
        filepath = self._get_filepath(context)
        if context.step_key != "data_quality_results":
            if output.empty:
                context.log.warning(
                    "Output DataFrame is empty. Skipping write operation."
                )
                return
            self.adls_client.upload_pandas_to_adls_csv(filepath, output)
            context.log.info("uploaded csv")
        else:
            self.adls_client.upload_json_to_adls_json(filepath, output)
            context.log.info("uploaded json")

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

        if (
            context.upstream_output.step_key == "data_quality_results"
            and context.asset_key.to_user_string() == "data_quality_results"
        ):
            file = self.adls_client.download_adls_json_to_json(filepath)
            context.log.info(f"downloaded json: {file}")
            return file
        else:
            file = self.adls_client.download_adls_csv_to_pandas(filepath)
            context.log.info(f"downloaded csv: {file}")
            return file

    def _get_filepath(self, context):
        filepath = context.step_context.op_config["filepath"]

        parent_folder = context.step_context.op_config["dataset_type"]
        step = context.step_key

        destination_filepath = _get_filepath(filepath, parent_folder, step)

        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath
