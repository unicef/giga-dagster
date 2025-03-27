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

    silver = silver.select(*["school_id_govt", "school_id_giga"])
    silver = silver.withColumnRenamed("school_id_giga", "school_id_giga1")

    joined_df = bronze.alias("bronze").join(
        silver.alias("silver"), on="school_id_govt", how="left"
    )
    df = joined_df.withColumn(
        "dq_is_not_create", f.when(f.col("school_id_giga1").isNotNull(), 1).otherwise(0)
    )

    return df.drop("school_id_giga1")


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
            f.col(silver_giga_col).isNull(),
            1,  # No matching school_id_govt
        ).otherwise(
            f.when(
                # Check for mismatched school_id_giga when both exist
                ("school_id_giga" in bronze_columns)
                & (f.col("school_id_giga").isNotNull())
                & (f.col(silver_giga_col).isNotNull())
                & (f.col("school_id_giga") != f.col(silver_giga_col)),
                1,  # Mismatched school_id_giga
            ).otherwise(0)
        ),
    )


def _log_mismatches(
    df: sql.DataFrame, silver_giga_col: str, context: OpExecutionContext = None
):
    """Log any mismatched school_id_giga values."""
    if context:
        logger = get_context_with_fallback_logger(context)
        mismatched = df.filter(
            (f.col("dq_is_not_update") == 1)
            & (f.col(silver_giga_col).isNotNull())
            & ("school_id_giga" in df.columns)
            & (f.col("school_id_giga").isNotNull())
        )
        mismatched_count = mismatched.count()
        if mismatched_count > 0:
            logger.warning(
                f"Found {mismatched_count} rows with mismatched school_id_giga values"
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

    # Rename silver columns to avoid conflicts after join
    silver_renamed, renamed_columns = _rename_silver_columns(silver)

    # Join to get existing data
    joined_df = bronze.join(silver_renamed, on="school_id_govt", how="left")

    # Mark rows that aren't valid updates
    silver_giga_col = renamed_columns.get("school_id_giga", "silver_school_id_giga")

    # Check update validity
    df = _check_update_validity(joined_df, silver_giga_col, bronze.columns)

    # Log mismatches
    _log_mismatches(df, silver_giga_col, context)

    # Fill null values for valid updates
    df = _fill_null_values(df, bronze.columns, silver.columns, renamed_columns)

    # Add school_id_giga from silver for valid updates if it's not in bronze
    if "school_id_giga" not in bronze.columns and "school_id_giga" in silver.columns:
        silver_giga_col = renamed_columns.get("school_id_giga")
        df = df.withColumn(
            "school_id_giga",
            f.when(f.col("dq_is_not_update") == 0, f.col(silver_giga_col)).otherwise(
                None
            ),
        )

    # Drop all silver renamed columns to avoid duplicates
    for col in renamed_columns.values():
        if col in df.columns:
            df = df.drop(col)

    logger.info("Completed update checks with field filling")
    return df
