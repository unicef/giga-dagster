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

    null_coords = (
        f.col("latitude").isNull()
        | f.isnan(f.col("latitude"))
        | f.col("longitude").isNull()
        | f.isnan(f.col("longitude"))
    )

    column_actions = {}
    for column_set in config_column_list:
        set_name = "_".join(column_set)
        window_count = f.count("*").over(Window.partitionBy(column_set))
        column_actions[f"dq_duplicate_set-{set_name}"] = (
            f.when(null_coords, f.lit(None).cast("int"))
            .when(window_count > 1, 1)
            .otherwise(0)
        )
        if list(column_set) == ["location_id"]:
            column_actions["dq_duplicate_location_rows_count"] = f.when(
                null_coords, f.lit(None).cast("int")
            ).otherwise(window_count)
            column_actions["dq_duplicate_location_rows_id"] = f.when(
                null_coords, f.lit(None)
            ).otherwise(f.substring(f.md5(f.col("location_id")), 1, 8))

    df = df.withColumns(column_actions)
    return df.drop("location_id")


def duplicate_all_except_checks(
    df: sql.DataFrame,
    config_column_list: list[str],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate all except checks...")

    existing_columns = [col for col in config_column_list if col in df.columns]

    count_expr = f.count("*").over(Window.partitionBy(existing_columns)) > 1

    if "latitude" in existing_columns and "longitude" in existing_columns:
        null_guard = (
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("longitude"))
        )
        result_expr = (
            f.when(null_guard, f.lit(None).cast("int")).when(count_expr, 1).otherwise(0)
        )
    else:
        result_expr = f.when(count_expr, 1).otherwise(0)

    df = df.withColumn("dq_duplicate_all_except_school_code", result_expr)

    return df
