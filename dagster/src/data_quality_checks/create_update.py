import uuid

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
    """Rename silver columns to avoid conflicts after join.

    Keeps school_id_govt and school_id_giga with original names for join/prioritization.
    Other columns get prefixed with 'silver_'.
    """
    silver_renamed = silver
    silver_columns = silver.columns
    renamed_columns_map = {}

    for col_name in silver_columns:
        if col_name not in ["school_id_govt", "school_id_giga"]:
            new_name = f"silver_{col_name}"
            # Check if new_name already exists to prevent Spark error
            if new_name not in silver_renamed.columns:
                silver_renamed = silver_renamed.withColumnRenamed(col_name, new_name)
                renamed_columns_map[col_name] = new_name
            else:
                # Handle cases where prefix might already exist (unlikely but safe)
                temp_name = f"silver_{col_name}_{uuid.uuid4().hex[:4]}"
                silver_renamed = silver_renamed.withColumnRenamed(col_name, temp_name)
                renamed_columns_map[col_name] = temp_name

        elif col_name == "school_id_giga":
            renamed_columns_map[col_name] = col_name  # Keep track, no rename
        # school_id_govt is the join key, implicitly kept track of, no rename needed

    return silver_renamed, renamed_columns_map


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


def update_checks(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running update checks with immediate select resolution...")

    # 1. Prepare Silver DataFrame
    silver_cols_to_select = ["school_id_govt"]
    if "school_id_giga" in silver.columns:
        silver_cols_to_select.append("school_id_giga")

    common_cols_for_coalesce = [
        col
        for col in bronze.columns
        if col in silver.columns and col not in ["school_id_govt", "school_id_giga"]
    ]
    silver_cols_to_select.extend(common_cols_for_coalesce)
    silver_cols_to_select = list(dict.fromkeys(silver_cols_to_select))  # Ensure unique

    if not silver_cols_to_select:
        logger.warning(
            "Silver columns to select is empty, cannot perform update checks effectively."
        )
        # Return bronze with a dummy DQ column? Or raise error?
        # For now, add DQ column assuming no silver match
        return bronze.withColumn("dq_is_not_update", f.lit(1))

    silver_minimal = silver.select(*[f.col(c) for c in silver_cols_to_select])
    silver_renamed, renamed_columns_map = _rename_silver_columns(silver_minimal)

    # 2. Perform Join
    joined_df = bronze.alias("bronze").join(
        silver_renamed.alias("silver"),
        on="school_id_govt",
        how="left",
    )

    # 3. Build Select Expression List for Immediate Resolution
    final_select_expr = []

    # DQ Check Flag
    final_select_expr.append(
        f.when(f.col("silver.school_id_govt").isNull(), 1)
        .otherwise(0)
        .alias("dq_is_not_update")
    )

    # Handle school_id_giga prioritization
    bronze_has_giga = "school_id_giga" in bronze.columns
    silver_has_giga = "school_id_giga" in silver_renamed.columns

    if bronze_has_giga and silver_has_giga:
        # Prioritize Silver's Giga ID if it exists and is valid update
        final_select_expr.append(
            f.when(
                (f.col("silver.school_id_govt").isNotNull())  # Check if join matched
                & f.col("silver.school_id_giga").isNotNull(),
                f.col("silver.school_id_giga"),
            )
            .otherwise(f.col("bronze.school_id_giga"))  # Fallback to Bronze's Giga ID
            .alias("school_id_giga")
        )
    elif bronze_has_giga:
        # Only bronze has giga, use it
        final_select_expr.append(f.col("bronze.school_id_giga").alias("school_id_giga"))
    elif silver_has_giga:
        # Only silver has giga, use it only if the join matched
        final_select_expr.append(
            f.when(
                f.col("silver.school_id_govt").isNotNull(),
                f.col("silver.school_id_giga"),
            )
            .otherwise(
                f.lit(None).cast(
                    bronze.schema["school_id_giga"].dataType
                    if bronze_has_giga
                    else silver.schema["school_id_giga"].dataType
                )
            )  # Ensure correct type
            .alias("school_id_giga")
        )
    # else: Neither has school_id_giga, so it won't be selected

    # Handle other columns (Coalesce for common, direct select for bronze-only)
    for col_name in bronze.columns:
        if col_name == "school_id_giga":  # Already handled above
            continue
        elif col_name == "school_id_govt":  # Use bronze's as the definitive one
            final_select_expr.append(
                f.col("bronze.school_id_govt").alias("school_id_govt")
            )
        elif (
            col_name in renamed_columns_map
        ):  # Common column, potentially fill nulls from silver
            silver_col_name = renamed_columns_map[col_name]
            final_select_expr.append(
                f.coalesce(
                    f.col(f"bronze.{col_name}"), f.col(f"silver.{silver_col_name}")
                ).alias(col_name)
            )
        else:  # Column only exists in bronze
            final_select_expr.append(f.col(f"bronze.{col_name}").alias(col_name))

    # 4. Execute Select
    # Use select instead of selectExpr for safety with generated column names
    final_df = joined_df.select(*final_select_expr)

    # Optional: Log mismatches based on final_df if needed
    # _log_mismatches(final_df, 'school_id_giga', context)

    logger.info("Completed update checks via immediate select resolution.")
    return final_df
