from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSJSONIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: dict):
        filepath = self._get_filepath(context)
        adls_client.upload_json(output, filepath)

        context.log.info(
            f"Uploaded {filepath.split('/')[-1]} to"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )

    def load_input(self, context: InputContext) -> list:
        filepath = self._get_filepath(context.upstream_output)
        data = adls_client.download_json(filepath)

        context.log.info(
            f"Downloaded {filepath.split('/')[-1]} from"
            f" {'/'.join(filepath.split('/')[:-1])} in ADLS."
        )
        return data
