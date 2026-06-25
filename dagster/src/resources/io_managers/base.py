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
            # asset_identifier matches an op_config key directly only when
            # the upstream op has a single output (op name == asset name).
            # For a @multi_asset producer, fall back to the upstream op's
            # own step_key instead of the deprecated, unreliable
            # context.upstream_output.step_context.
            asset_identifier, *_ = context.get_asset_identifier()
            run_config = context.step_context.run_config
            op_config = run_config["ops"].get(asset_identifier)
            if op_config is None:
                op_config = run_config["ops"].get(context.upstream_output.step_key)

            op_config = op_config["config"]
            config = FileConfig(**op_config)
            if config.output_filepaths and context.name in config.output_filepaths:
                return Path(config.output_filepaths[context.name])
            return config.destination_filepath_object

        config = FileConfig(**context.step_context.op_config)
        if config.output_filepaths and context.name in config.output_filepaths:
            return Path(config.output_filepaths[context.name])
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
