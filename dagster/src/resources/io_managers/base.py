from abc import ABC

from dagster import ConfigurableIOManager, InputContext, OutputContext
from src.utils.adls import get_filepath


class BaseConfigurableIOManager(ConfigurableIOManager, ABC):
    @staticmethod
    def _get_filepath(context: InputContext | OutputContext):
        filepath = context.step_context.op_config["filepath"]

        parent_folder = context.step_context.op_config["dataset_type"]
        step = context.step_key

        destination_filepath = get_filepath(filepath, parent_folder, step)

        context.log.info(
            f"Original filepath: {filepath}, new filepath: {destination_filepath}"
        )

        return destination_filepath
