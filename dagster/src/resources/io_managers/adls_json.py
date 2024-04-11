from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSJSONIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: dict | list[dict]):
        path = self._get_filepath(context)
        adls_client.upload_json(output, str(path))

        context.log.info(f"Uploaded {path.name} to" f" {path.parent} in ADLS.")

    def load_input(self, context: InputContext) -> dict | list[dict]:
        path = self._get_filepath(context)
        data = adls_client.download_json(str(path))

        context.log.info(f"Downloaded {path.name} from" f" {path.parent} in ADLS.")
        return data
