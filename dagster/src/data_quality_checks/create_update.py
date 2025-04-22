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
    renamed_columns_map = {}

    for col_name in silver_columns:
        # Don't rename the join key or the target column
        if col_name not in ["school_id_govt", "school_id_giga"]:
            new_name = f"silver_{col_name}"
            silver_renamed = silver_renamed.withColumnRenamed(col_name, new_name)
            renamed_columns_map[col_name] = new_name
        elif (
            col_name == "school_id_giga"
        ):  # Keep track of the original giga id column name
            renamed_columns_map[col_name] = col_name

    return silver_renamed, renamed_columns_map


def _check_update_validity(
    joined_df: sql.DataFrame, silver_giga_col: str, bronze_columns: list[str]
) -> sql.DataFrame:
    """Check if updates are valid."""
    # Use the original silver school_id_govt column for the check, which wasn't renamed
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
    renamed_columns_map: dict[str, str],
) -> sql.DataFrame:
    """Fill null values for valid updates from silver data."""
    for col_name in bronze_cols:
        # Skip school_id_giga as it's handled by prioritization logic
        if col_name == "school_id_giga":
            continue

        if col_name != "school_id_govt" and col_name in silver_columns:
            # Use the map to find the corresponding (potentially renamed) silver column
            silver_col = renamed_columns_map.get(col_name)
            if silver_col:
                df = df.withColumn(
                    col_name,
                    f.when(
                        (f.col("dq_is_not_update") == 0) & f.col(col_name).isNull(),
                        f.col(
                            f"silver.{silver_col}"
                        ),  # Reference the silver column correctly
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

    # Select only necessary columns from silver and rename them
    silver_cols_to_select = ["school_id_govt", "school_id_giga"] + [
        col
        for col in bronze.columns
        if col in silver.columns and col != "school_id_govt"
    ]
    silver_minimal = silver.select(*[f.col(c) for c in silver_cols_to_select])
    silver_renamed, renamed_columns_map = _rename_silver_columns(silver_minimal)

    # Join bronze with renamed silver
    # Use alias for clarity
    joined_df = bronze.alias("bronze").join(
        silver_renamed.alias("silver"),
        on="school_id_govt",  # Join key is not renamed
        how="left",
    )

    # Mark rows that aren't valid updates (no match in silver)
    df_checked = _check_update_validity(joined_df, "school_id_giga", bronze.columns)

    # Log mismatches
    _log_mismatches(df_checked, "school_id_giga", context)

    # Prioritize silver school_id_giga for valid updates
    # Ensure the column exists before trying to overwrite
    if "school_id_giga" in bronze.columns and "school_id_giga" in silver.columns:
        df_prioritized = df_checked.withColumn(
            "school_id_giga",
            f.when(
                (f.col("dq_is_not_update") == 0)
                & f.col("silver.school_id_giga").isNotNull(),
                f.col("silver.school_id_giga"),  # Use silver's giga id
            ).otherwise(
                f.col("bronze.school_id_giga")  # Keep bronze's giga id
            ),
        )
    else:
        # If school_id_giga is not in bronze or silver, just keep the checked df
        df_prioritized = df_checked

    # Explicitly drop the potentially ambiguous silver.school_id_giga BEFORE filling nulls
    # This ensures f.col("school_id_giga") in _fill_null_values is unambiguous
    if "silver.school_id_giga" in df_prioritized.columns:
        df_prioritized = df_prioritized.drop("silver.school_id_giga")

    # Fill other null values for valid updates using the renamed silver columns map
    df_filled = _fill_null_values(
        df_prioritized,
        bronze.columns,
        silver_minimal.columns,  # Original silver column names
        renamed_columns_map,  # Map to potentially renamed silver columns
    )

    # Drop the potentially renamed silver columns (except the prioritized school_id_giga)
    cols_to_drop = [
        f"silver.{renamed_columns_map[col]}"
        for col in silver_minimal.columns
        if col in renamed_columns_map and col != "school_id_giga"
    ]
    # Also drop the original silver join key if it wasn't selected in bronze initially
    if "silver.school_id_govt" in df_filled.columns:
        cols_to_drop.append("silver.school_id_govt")

    final_df = df_filled.drop(*cols_to_drop)

    logger.info("Completed update checks")
    return final_df
