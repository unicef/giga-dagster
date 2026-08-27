from pyspark import sql
from pyspark.sql import (
    Window,
    functions as f,
)

from dagster import OpExecutionContext
from src.data_quality_checks.location_grouping import (
    GROUP_COUNT_COLUMN,
    add_group_counts,
    location_duplicate_columns,
    location_id_column,
    null_coordinates,
)
from src.utils.logger import get_context_with_fallback_logger


def duplicate_set_checks(
    df: sql.DataFrame,
    config_column_list: set[str],
    context: OpExecutionContext = None,
    reference: sql.DataFrame = None,
):
    """Flag rows sharing every column in each configured set.

    ``reference`` rows (silver schools absent from the upload) take part in the
    counts without appearing in the output, so a school duplicating one already in
    the dataset is caught.
    """
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate set checks...")

    has_lat_long = {"latitude", "longitude"}.issubset(df.columns)

    if has_lat_long:
        df = df.withColumn("location_id", location_id_column())
        null_coords = null_coordinates(df)
        if reference is not None:
            reference = reference.withColumn("location_id", location_id_column())
    else:
        null_coords = f.lit(False)
        reference = None

    column_actions = {}
    count_columns = []
    for column_set in config_column_list:
        required_columns = set(column_set) - {"location_id"}
        needs_location_id = "location_id" in column_set
        if not required_columns.issubset(df.columns) or (
            needs_location_id and not has_lat_long
        ):
            logger.info(
                f"Skipping duplicate set check for {column_set} — missing columns"
            )
            continue

        is_location_only = list(column_set) == ["location_id"]
        flag_col = (
            "dq_duplicate_location_rows_flag"
            if is_location_only
            else f"dq_duplicate_set-{'_'.join(column_set)}"
        )

        set_reference = reference
        if set_reference is not None and not set(column_set).issubset(
            set_reference.columns
        ):
            logger.info(
                f"Duplicate set check for {column_set} stays file-scoped — "
                "reference is missing columns"
            )
            set_reference = None

        file_count = f.count("*").over(Window.partitionBy(column_set))
        if set_reference is None:
            count_col = file_count
        else:
            count_column_name = f"_count-{flag_col}"
            df = add_group_counts(
                df, set_reference, list(column_set)
            ).withColumnRenamed(GROUP_COUNT_COLUMN, count_column_name)
            count_columns.append(count_column_name)
            count_col = f.col(count_column_name)

        if is_location_only:
            column_actions.update(location_duplicate_columns(count_col, null_coords))
            # Only answerable against a reference; without one the check never ran.
            column_actions["dq_duplicate_location_rows_in_dataset"] = (
                f.lit(None).cast("int")
                if set_reference is None
                else f.when(null_coords, f.lit(None).cast("int"))
                .when(count_col > file_count, 1)
                .otherwise(0)
            )
        else:
            column_actions[flag_col] = (
                f.when(null_coords, f.lit(None).cast("int"))
                .when(count_col > 1, 1)
                .otherwise(0)
            )

    df = df.withColumns(column_actions).drop(*count_columns)
    return df.drop("location_id") if has_lat_long else df


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
