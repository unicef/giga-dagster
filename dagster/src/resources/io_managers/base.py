from abc import ABC
from collections.abc import Callable
from pathlib import Path

from pyspark.sql import DataFrame

from dagster import ConfigurableIOManager, InputContext, OutputContext
from src.settings import settings
from src.utils.op_config import FileConfig
from src.utils.spark import (
    transform_qos_bra_types,
    transform_school_types,
)


class BaseConfigurableIOManager(ConfigurableIOManager, ABC):
    @staticmethod
    def _get_filepath(context: InputContext | OutputContext) -> Path:
        if isinstance(context, InputContext):
            # Trick to get the correct upstream step context
            #
            # context.upstream_output.step_context returns the op_config
            # of the current step when it has more than 1 upstream assets
            asset_identifier, *_ = (
                context.get_asset_identifier()
            )  # this already points to the correct upstream
            run_config = context.step_context.run_config
            op_config = run_config["ops"].get(asset_identifier)

            if op_config is None:
                op_config = context.upstream_output.step_context.op_config
                config = FileConfig(**op_config)
                return config.filepath_object

            op_config = op_config["config"]
            config = FileConfig(**op_config)
            return config.destination_filepath_object

        config = FileConfig(**context.step_context.op_config)
        return config.destination_filepath_object

    @staticmethod
    def _get_schema_name(context: OutputContext):
        config: FileConfig = context.step_context.op_config
        return config.metastore_schema

    @staticmethod
    def _get_table_path(context: OutputContext, filepath: str):
        if (
            "gold" not in context.step_key
            or "silver" not in context.step_key
            or "staging" not in context.step_key
        ):
            table_name = filepath.split("/")[-1].split(".")[0]
        else:
            table_name = filepath.split("/")[-1].split("_")[1]

        return f"{settings.AZURE_DFS_CONNECTION_URI}/{'/'.join(filepath.split('/')[:-1])}/{table_name}"

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
