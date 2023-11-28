from pyspark.sql import DataFrame

from dagster import InputContext, IOManager, OutputContext
from src._utils.adls import ADLSFileClient, _get_filepath


class StagingADLSIOManager(IOManager):
    def __init__(self):
        self.adls_client = ADLSFileClient()

    def handle_output(self, context: OutputContext, output: DataFrame):
        filepath = self._get_filepath(context)
        if context.step_key != "data_quality_results":
            if output.isEmpty():
                context.log.warning(
                    "Output DataFrame is empty. Skipping write operation."
                )
                return
            self.adls_client.upload_spark_dataframe_to_adls_deltatable(output, filepath)
        else:
            self.adls_client.upload_json_to_adls_json(filepath, output)

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {('/').join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext):
        filepath = self._get_filepath(context.upstream_output)

        if (
            context.upstream_output.step_key == "data_quality_results"
            and context.asset_key.to_user_string() == "data_quality_results"
        ):
            file = self.adls_client.download_adls_json_to_json(filepath)
        else:
            file = self.adls_client.download_adls_deltatable_to_spark_dataframe(
                filepath
            )

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {('/').join(filepath.split('/')[:-1])} in ADLS."
        )

        return file

    def _get_filepath(self, context):
        filepath = context.step_context.op_config["filepath"]

        parent_folder = context.step_context.op_config["dataset_type"]
        step = context.step_key

        destination_filepath = _get_filepath(filepath, parent_folder, step)

        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath
