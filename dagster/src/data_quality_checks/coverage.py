from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.utils.logger import get_context_with_fallback_logger


def fb_percent_sum_to_100_check(df: sql.DataFrame, context: OpExecutionContext = None):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running sum of percent not equal 100 checks...")

    df = df.withColumn(
        "dq_is_sum_of_percent_not_equal_100",
        f.when(
            f.col("percent_2G") + f.col("percent_3G") + f.col("percent_4G") != 100,
            1,
        ).otherwise(0),
    )
    return df


def school_exists_in_master_check(
    df: sql.DataFrame,
    silver: sql.DataFrame | None,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running school exists in school master checks...")

    if silver is None:
        logger.warning("No geolocation silver table; flagging every row.")
        return df.withColumn("dq_school_not_in_school_master", f.lit(1))

    known_ids = silver.select(f.col("school_id_giga").alias("_known_id")).distinct()
    return (
        df.join(known_ids, df["school_id_giga"] == known_ids["_known_id"], "left")
        .withColumn(
            "dq_school_not_in_school_master",
            f.when(f.col("_known_id").isNull(), 1).otherwise(0),
        )
        .drop("_known_id")
    )
