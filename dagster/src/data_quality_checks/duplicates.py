from pyspark import sql
from pyspark.sql import (
    Window,
    functions as f,
)

from dagster import OpExecutionContext
from src.utils.logger import get_context_with_fallback_logger


def duplicate_set_checks(
    df: sql.DataFrame,
    config_column_list: set[str],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate set checks...")

    df = df.withColumn(
        "location_id",
        f.concat_ws(
            "_",
            f.col("longitude").cast("string"),
            f.col("latitude").cast("string"),
        ),
    )

    column_actions = {}
    for column_set in config_column_list:
        set_name = "_".join(column_set)
        column_actions[f"dq_duplicate_set-{set_name}"] = (
            f.when(
                f.col("latitude").isNull()
                | f.isnan(f.col("latitude"))
                | f.col("longitude").isNull()
                | f.isnan(f.col("latitude")),
                f.lit(None).cast("int"),
            )
            .when(
                f.count("*").over(Window.partitionBy(column_set)) > 1,
                1,
            )
            .otherwise(0)
        )

    df = df.withColumns(column_actions)
    return df.drop("location_id")


def duplicate_all_except_checks(
    df: sql.DataFrame,
    config_column_list: list[str],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate all except checks...")

    df = df.withColumn(
        "dq_duplicate_all_except_school_code",
        f.when(
            f.count("*").over(Window.partitionBy(config_column_list)) > 1,
            1,
        ).otherwise(0),
    )

    return df
