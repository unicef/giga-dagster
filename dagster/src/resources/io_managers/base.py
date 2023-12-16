from abc import ABC
from typing import Callable

from pyspark.sql import DataFrame

from dagster import ConfigurableIOManager, InputContext, OutputContext
from src.utils.adls import get_filepath
from src.utils.spark import (
    transform_school_master_types,
    transform_school_reference_types,
)


class BaseConfigurableIOManager(ConfigurableIOManager, ABC):
    @staticmethod
    def _get_filepath(context: InputContext | OutputContext):
        filepath = context.step_context.op_config["filepath"]

        parent_folder = context.step_context.op_config["dataset_type"]
        step = context.step_key

        destination_filepath = get_filepath(filepath, parent_folder, step)

        context.log.info(f"Moving from {filepath} to {destination_filepath}")

        return destination_filepath

    @staticmethod
    def _get_schema(context: InputContext | OutputContext):
        return context.step_context.op_config["metastore_schema"]

    @staticmethod
    def _get_table_schema_definition(context: InputContext | OutputContext):
        return context.step_context.op_config["table_schema_definition"]

    @staticmethod
    def _get_type_transform_function(
        context: InputContext | OutputContext,
    ) -> Callable[[DataFrame, OutputContext | None], DataFrame]:
        dataset_type = context.step_context.op_config["dataset_type"]
        # TODO: Add the correct transform functions for the other datasets/layers
        if dataset_type == "school-reference":
            return transform_school_reference_types
        return transform_school_master_types
