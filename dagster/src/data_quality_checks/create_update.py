from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.utils.logger import get_context_with_fallback_logger


def create_checks(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running create checks...")

    # Check if school_id_govt already exists in silver
    silver = silver.select("school_id_govt")
    joined_df = bronze.alias("bronze").join(
        silver.alias("silver"), on="school_id_govt", how="left"
    )
    df = joined_df.withColumn(
        "dq_is_not_create",
        f.when(f.col("silver.school_id_govt").isNotNull(), 1).otherwise(0),
    )

    return df.drop("silver.school_id_govt")


def _rename_silver_columns(
    silver: sql.DataFrame,
) -> tuple[sql.DataFrame, dict[str, str]]:
    """Rename silver columns to avoid conflicts after join."""
    silver_renamed = silver
    silver_columns = silver.columns
    renamed_columns = {}

    for col in silver_columns:
        if col != "school_id_govt":  # Don't rename the join key
            new_name = f"silver_{col}"
            silver_renamed = silver_renamed.withColumnRenamed(col, new_name)
            renamed_columns[col] = new_name

    return silver_renamed, renamed_columns


def _check_update_validity(
    joined_df: sql.DataFrame, silver_giga_col: str, bronze_columns: list[str]
) -> sql.DataFrame:
    """Check if updates are valid."""
    return joined_df.withColumn(
        "dq_is_not_update",
        f.when(
            f.col("silver.school_id_govt").isNull(),
            1,  # No matching school_id_govt
        ).otherwise(0),
    )


def _log_mismatches(
    df: sql.DataFrame, silver_giga_col: str, context: OpExecutionContext = None
):
    """Log any mismatched school_id_giga values."""
    if context:
        logger = get_context_with_fallback_logger(context)
        mismatched = df.filter(f.col("dq_is_not_update") == 1)
        mismatched_count = mismatched.count()
        if mismatched_count > 0:
            logger.warning(
                f"Found {mismatched_count} rows with no matching school_id_govt in silver"
            )
            logger.warning("These records will be flagged as invalid updates")


def _fill_null_values(
    df: sql.DataFrame,
    bronze_cols: list[str],
    silver_columns: list[str],
    renamed_columns: dict[str, str],
) -> sql.DataFrame:
    """Fill null values for valid updates from silver data."""
    for col_name in bronze_cols:
        if col_name != "school_id_govt" and col_name in silver_columns:
            silver_col = renamed_columns.get(col_name)
            if silver_col:
                df = df.withColumn(
                    col_name,
                    f.when(
                        (f.col("dq_is_not_update") == 0) & f.col(col_name).isNull(),
                        f.col(silver_col),
                    ).otherwise(f.col(col_name)),
                )
    return df


def update_checks(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running update checks...")

    # Join to get existing data
    joined_df = bronze.join(silver, on="school_id_govt", how="left")

    # Mark rows that aren't valid updates
    df = _check_update_validity(joined_df, "school_id_giga", bronze.columns)

    # Log mismatches
    _log_mismatches(df, "school_id_giga", context)

    # Fill null values for valid updates
    df = _fill_null_values(df, bronze.columns, silver.columns, {})

    logger.info("Completed update checks with field filling")
    return df
