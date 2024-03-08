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
