from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_lineage import emit_lineage

adls_client = ADLSFileClient()


class ADLSPassthroughIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: bytes):
        # Write nothing. This is just needed to materialize the asset in Dagster.

        context.log.info(
            f"EMIT LINEAGE CALLED FROM IO MANAGER: {self.__class__.__name__}"
        )
        emit_lineage(context=context)
        pass

    def load_input(self, context: InputContext) -> bytes:
        path = self._get_filepath(context)
        context.log.info(f"Downloading {path}...")
        data = adls_client.download_raw(str(path))
        context.log.info(f"Downloaded {str(path)} from ADLS.")

        return data
