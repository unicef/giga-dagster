import datahub.emitter.mce_builder as builder

from dagster import InputContext, OutputContext
from src.resources.io_managers.base import BaseConfigurableIOManager
from src.utils.adls import ADLSFileClient
from src.utils.datahub.emit_lineage import emit_lineage

adls_client = ADLSFileClient()


class ADLSPassthroughIOManager(BaseConfigurableIOManager):
    def handle_output(self, context: OutputContext, output: bytes):
        pass

    def load_input(self, context: InputContext) -> bytes:
        filepath = context.step_context.op_config["filepath"]
        data = adls_client.download_raw(filepath)
        context.log.info(f"Downloaded {filepath} from ADLS.")

        current_filepath = self._get_filepath_from_InputContext(context)
        context.log.info(f"current_filepath: {current_filepath}")
        platform = builder.make_data_platform_urn("adlsGen2")
        emit_lineage(
            context,
            dataset_filepath=current_filepath,
            upstream_filepath=filepath,
            platform=platform,
        )

        return data
