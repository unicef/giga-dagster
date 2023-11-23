import pandas as pd

from dagster import InputContext, IOManager, OutputContext

# from src._utils import get_spark_session
from src.resources._utils import get_destination_filepath
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

        # if context.step_key == "gold":
        #     self._create_delta_table(context, output, filepath)

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


# no type
# education_level_regional VARCHAR(20)
# school_type VARCHAR(20)
#  admin1 VARCHAR(100)
#                 admin2 VARCHAR(100)
#                 admin3 VARCHAR(100)
#                 # admin4 VARCHAR(100)
