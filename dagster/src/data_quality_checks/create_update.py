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

    silver = silver.select(*["school_id_govt", "school_id_giga"])
    silver = silver.withColumnRenamed("school_id_giga", "school_id_giga1")

    joined_df = bronze.alias("bronze").join(
        silver.alias("silver"), on="school_id_govt", how="left"
    )
    df = joined_df.withColumn(
        "dq_is_not_update", f.when(f.col("school_id_giga1").isNull(), 1).otherwise(0)
    )

    return df.drop("school_id_giga1")
