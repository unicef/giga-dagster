from azure.core.exceptions import ResourceNotFoundError
from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient

adls_client = ADLSFileClient()


class ADLSPassthroughIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: bytes):
        # Write nothing. This is just needed to materialize the asset in Dagster.
        pass

    def load_input(self, context: InputContext) -> bytes | None:
        path = self._get_filepath(context)
        context.log.info(f"Downloading {path}...")
        try:
            data = adls_client.download_raw(str(path))
        except ResourceNotFoundError as e:
            if "_reference_" in context.asset_key.to_user_string():
                context.log.info(f"Reference file {path!s} does not exist.")
                return b""
            raise e

        context.log.info(f"Downloaded {path!s} from ADLS.")
        return data
