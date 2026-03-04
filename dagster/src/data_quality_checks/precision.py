from typing import Any

from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.spark.user_defined_functions import get_decimal_places_updated
from src.utils.logger import get_context_with_fallback_logger


def precision_check(
    df: sql.DataFrame,
    config_column_list: dict[str, Any],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running precision checks...")

    column_actions = {}
    for column in config_column_list:
        # precision = config_column_list[column]["min"]
        # get_decimal_places = get_decimal_places_udf_factory(precision)
        column_actions[f"dq_precision-{column}"] = get_decimal_places_updated(
            f.col(column)
        )

    return df.withColumns(column_actions)
