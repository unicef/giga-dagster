from pyspark import sql

from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_dataset_metadata import emit_metadata_to_datahub

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSJSONIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: sql.DataFrame):
        filepath = self._get_filepath(context)
        if output.isEmpty():
            context.log.warning("Output DataFrame is empty. Skipping write operation.")
            return

        adls_client.upload_json(output.toPandas().to_json(), filepath)

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

        context.log.info("EMITTING METADATA TO DATAHUB")
        input_filepath = context.step_context.op_config["filepath"]
        context.log.info(f"Input Filepath: {input_filepath}")
        context.log.info(f"Output Filepath: {filepath}")
        emit_metadata_to_datahub(
            context, output_filepath=filepath, input_filepath=input_filepath, df=output
        )

        context.log.info(
            f"Metadata of {filepath.split('/')[-1]} has been successfully emitted to Datahub."
        )

    def load_input(self, context: InputContext) -> list:
        filepath = self._get_filepath(context.upstream_output)
        data = adls_client.download_json(filepath)

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )
        return data
