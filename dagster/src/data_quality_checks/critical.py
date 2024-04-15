from functools import reduce
from operator import add

from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.utils.logger import get_context_with_fallback_logger


def critical_error_checks(
    df: sql.DataFrame,
    dataset_type: str,
    config_column_list: list[str],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running critical error checks...")

    critial_column_dq_checks = [
        f.col(f"dq_is_null_mandatory-{column}") for column in config_column_list
    ]

    if dataset_type == "master":
        critial_column_dq_checks.extend(
            [
                f.col("dq_duplicate-school_id_govt"),
                f.col("dq_duplicate-school_id_giga"),
                f.col("dq_is_invalid_range-latitude"),
                f.col("dq_is_invalid_range-longitude"),
                # f.col("dq_is_not_within_country"),
            ],
        )

    df = df.withColumn(
        "dq_has_critical_error",
        f.when(
            reduce(add, critial_column_dq_checks) > 0,
            1,
        ).otherwise(0),
    )

    return df
