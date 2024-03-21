from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient

adls_client = ADLSFileClient()


class ADLSPassthroughIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: bytes):
        # Write nothing. This is just needed to materialize the asset in Dagster.
        pass

    def load_input(self, context: InputContext) -> bytes:
        path = self._get_filepath(context)
        context.log.info(f"Downloading {path}...")
        data = adls_client.download_raw(str(path))
        context.log.info(f"Downloaded {str(path)} from ADLS.")

        # current_filepath = self._get_filepath_from_InputContext(context)
        # context.log.info(f"current_filepath: {current_filepath}")
        # platform = builder.make_data_platform_urn("adlsGen2")
        # emit_lineage(
        #     context,
        #     dataset_filepath=current_filepath,
        #     upstream_filepath=filepath,
        #     platform=platform,
        # )

        return data
