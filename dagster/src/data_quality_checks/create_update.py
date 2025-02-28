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


def update_checks(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running update checks...")

    # Rename silver columns to avoid conflicts after join
    silver_renamed = silver
    silver_columns = silver.columns

    # Create a mapping of original column names to renamed column names
    renamed_columns = {}
    for col in silver_columns:
        if col != "school_id_govt":  # Don't rename the join key
            new_name = f"silver_{col}"
            silver_renamed = silver_renamed.withColumnRenamed(col, new_name)
            renamed_columns[col] = new_name

    # Join to get existing data
    joined_df = bronze.join(silver_renamed, on="school_id_govt", how="left")

    # Mark rows that aren't valid updates
    silver_giga_col = renamed_columns.get("school_id_giga", "silver_school_id_giga")
    df = joined_df.withColumn(
        "dq_is_not_update", f.when(f.col(silver_giga_col).isNull(), 1).otherwise(0)
    )

    # For valid updates, fill NULL values with existing values from silver
    # First, get a list of columns that exist in bronze
    bronze_cols = bronze.columns

    # For each bronze column, fill NULL values with existing values from silver
    # but only for rows that are valid updates (dq_is_not_update = 0)
    for col_name in bronze_cols:
        if (
            col_name != "school_id_govt" and col_name in silver_columns
        ):  # Don't fill the join key
            silver_col = renamed_columns.get(col_name)
            if silver_col:
                df = df.withColumn(
                    col_name,
                    f.when(
                        (f.col("dq_is_not_update") == 0) & f.col(col_name).isNull(),
                        f.col(silver_col),
                    ).otherwise(f.col(col_name)),
                )

    # Add school_id_giga from silver for valid updates if it's not in bronze
    if "school_id_giga" not in bronze_cols and "school_id_giga" in silver_columns:
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
