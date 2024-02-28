from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient

adls_client = ADLSFileClient()


class ADLSPassthroughIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: bytes):
        pass

    def load_input(self, context: InputContext) -> bytes:
        filepath = context.step_context.op_config["filepath"]
        data = adls_client.download_raw(filepath)
        context.log.info(f"Downloaded {filepath} from ADLS.")
        return data
