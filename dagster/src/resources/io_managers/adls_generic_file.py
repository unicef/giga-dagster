from dagster import InputContext, OutputContext
from src.utils.adls import ADLSFileClient

from .base import BaseConfigurableIOManager

adls_client = ADLSFileClient()


class ADLSGenericFileIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: bytes | str):
        path = self._get_filepath(context)

        if isinstance(output, str):
            output = output.encode()
        adls_client.upload_raw(context, output, str(path))

        context.log.info(f"Uploaded {path.name} to {path.parent} in ADLS.")

    def load_input(self, context: InputContext) -> dict | list[dict]:
        path = self._get_filepath(context)
        data = adls_client.download_raw(str(path))

        context.log.info(f"Downloaded {path.name} from {path.parent} in ADLS.")
        return data
