import uuid

from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.utils.logger import get_context_with_fallback_logger


def add_is_new_school(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Add is_new_school boolean column to bronze df.

    True  → school_id_govt not found in silver (new school to be created)
    False → school_id_govt already exists in silver (existing school to be updated)
    """
    logger = get_context_with_fallback_logger(context)
    logger.info("Deriving is_new_school from silver join...")

    silver_ids = silver.select("school_id_govt")
    joined = bronze.alias("bronze").join(
        silver_ids.alias("silver"), on="school_id_govt", how="left"
    )
    return joined.withColumn(
        "is_new_school",
        f.col("silver.school_id_govt").isNull(),
    ).select("bronze.*", "is_new_school")


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
            if new_name not in silver_renamed.columns:
                silver_renamed = silver_renamed.withColumnRenamed(col_name, new_name)
                renamed_columns_map[col_name] = new_name
            else:
                temp_name = f"silver_{col_name}_{uuid.uuid4().hex[:4]}"
                silver_renamed = silver_renamed.withColumnRenamed(col_name, temp_name)
                renamed_columns_map[col_name] = temp_name
        elif col_name == "school_id_giga":
            renamed_columns_map[col_name] = col_name

    return silver_renamed, renamed_columns_map


def enrich_with_silver_values(
    bronze: sql.DataFrame,
    silver: sql.DataFrame,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Fill gaps in bronze with silver values for existing schools.

    For each row where school_id_govt matches silver:
    - Prioritises silver's school_id_giga (authoritative)
    - Coalesces each common column: bronze value first, silver as fallback

    New schools (no silver match) are returned unchanged.
    Does not produce any DQ flags.
    """
    logger = get_context_with_fallback_logger(context)
    logger.info("Enriching bronze with silver values for existing schools...")

    silver_cols_to_select = ["school_id_govt"]
    if "school_id_giga" in silver.columns:
        silver_cols_to_select.append("school_id_giga")

    common_cols_for_coalesce = [
        col
        for col in bronze.columns
        if col in silver.columns
        and col not in ["school_id_govt", "school_id_giga", "is_new_school"]
    ]
    silver_cols_to_select.extend(common_cols_for_coalesce)
    silver_cols_to_select = list(dict.fromkeys(silver_cols_to_select))

    silver_minimal = silver.select(*[f.col(c) for c in silver_cols_to_select])
    silver_renamed, renamed_columns_map = _rename_silver_columns(silver_minimal)

    joined_df = bronze.alias("bronze").join(
        silver_renamed.alias("silver"),
        on="school_id_govt",
        how="left",
    )

    final_select_expr = []

    # school_id_giga: prefer silver's value for existing schools
    bronze_has_giga = "school_id_giga" in bronze.columns
    silver_has_giga = "school_id_giga" in silver_renamed.columns

    if bronze_has_giga and silver_has_giga:
        final_select_expr.append(
            f.when(
                f.col("silver.school_id_govt").isNotNull()
                & f.col("silver.school_id_giga").isNotNull(),
                f.col("silver.school_id_giga"),
            )
            .otherwise(f.col("bronze.school_id_giga"))
            .alias("school_id_giga")
        )
    elif bronze_has_giga:
        final_select_expr.append(f.col("bronze.school_id_giga").alias("school_id_giga"))
    elif silver_has_giga:
        final_select_expr.append(
            f.when(
                f.col("silver.school_id_govt").isNotNull(),
                f.col("silver.school_id_giga"),
            )
            .otherwise(f.lit(None).cast(silver.schema["school_id_giga"].dataType))
            .alias("school_id_giga")
        )

    for col_name in bronze.columns:
        if col_name == "school_id_giga":
            continue
        elif col_name == "school_id_govt":
            final_select_expr.append(
                f.col("bronze.school_id_govt").alias("school_id_govt")
            )
        elif col_name in renamed_columns_map:
            silver_col_name = renamed_columns_map[col_name]
            final_select_expr.append(
                f.coalesce(
                    f.col(f"bronze.{col_name}"), f.col(f"silver.{silver_col_name}")
                ).alias(col_name)
            )
        else:
            final_select_expr.append(f.col(f"bronze.{col_name}").alias(col_name))

    logger.info("Silver enrichment complete.")
    return joined_df.select(*final_select_expr)
