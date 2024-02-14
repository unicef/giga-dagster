from abc import ABC
from collections.abc import Callable

from pyspark.sql import DataFrame

from dagster import ConfigurableIOManager, InputContext, OutputContext
from src.settings import settings
from src.utils.adls import get_filepath
from src.utils.spark import (
    transform_qos_bra_types,
    transform_school_types,
)


class BaseConfigurableIOManager(ConfigurableIOManager, ABC):
    @staticmethod
    def _get_filepath(context: InputContext | OutputContext):
        context.log.info(context.step_context.op_config["filepath"])
        filepath = context.step_context.op_config["filepath"]

        parent_folder = context.step_context.op_config["dataset_type"]
        step = context.step_key

        destination_filepath = get_filepath(filepath, parent_folder, step)

        context.log.info(
            f"Original filepath: {filepath}, new filepath: {destination_filepath}"
        )

        return destination_filepath

    @staticmethod
    def _get_schema(context: InputContext | OutputContext):
        return context.step_context.op_config["dataset_type"]

    @staticmethod
    def _get_table_path(context: OutputContext, filepath: str):
        if (
            "gold" not in context.step_key
            or "silver" not in context.step_key
            or "staging" not in context.step_key
        ):
            table_name = filepath.split("/")[-1].split(".")[0]
        else:
            table_name = filepath.split("/").split("_")[0]

        return f"{settings.AZURE_BLOB_CONNECTION_URI}/{'/'.join(filepath.split('/')[:-1])}/{table_name}"

    @staticmethod
    def _get_type_transform_function(
        context: InputContext | OutputContext,
    ) -> Callable[[DataFrame, OutputContext | None], DataFrame]:
        dataset_type = context.step_context.op_config["dataset_type"]
        # TODO: Add the correct transform functions for the other datasets/layers
        match dataset_type:
            case "qos":
                return transform_qos_bra_types
            case _:
                return transform_school_types
