from difflib import SequenceMatcher

from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.spark.config_expectations import config
from src.utils.logger import get_context_with_fallback_logger


def has_similar_name(df: sql.DataFrame, context: OpExecutionContext = None):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running has similar name checks...")

    name_list = df.select(f.col("school_name")).collect()
    name_list = [row["school_name"] for row in name_list]
    with_similar_name = []

    for index in range(len(name_list)):
        string_value = name_list.pop(index)
        if string_value in with_similar_name:
            name_list.insert(index, string_value)
            continue

        for name in name_list:
            if (
                SequenceMatcher(None, string_value, name).ratio()
                > config.SIMILARITY_RATIO_CUTOFF
            ):
                with_similar_name.append(string_value)
                with_similar_name.append(name)
                break

        name_list.insert(index, string_value)

    return df.withColumn(
        "dq_has_similar_name",
        f.when(f.col("school_name").isin(with_similar_name), 1).otherwise(0),
    )
