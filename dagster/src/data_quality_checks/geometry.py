from pyspark import sql
from pyspark.sql import (
    Window,
    functions as f,
)
from pyspark.sql.types import FloatType

from dagster import OpExecutionContext
from src.data_quality_checks.name_similarity import has_similar_name
from src.spark.user_defined_functions import h3_geo_to_h3_udf, point_110_udf
from src.utils.logger import (
    ContextLoggerWithLoguruFallback,
    get_context_with_fallback_logger,
)


def duplicate_name_level_110_check(
    df: sql.DataFrame, context: OpExecutionContext = None
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate level within 110m checks...")

    df_columns = df.columns

    df = df.withColumn("lat_110", point_110_udf(f.col("latitude")))
    df = df.withColumn("long_110", point_110_udf(f.col("longitude")))
    window_spec1 = Window.partitionBy(
        "school_name", "education_level", "lat_110", "long_110"
    )
    df = df.withColumn(
        "dq_duplicate_name_level_within_110m_radius",
        f.when(f.count("*").over(window_spec1) > 1, 1).otherwise(0),
    )

    added_columns = ["lat_110", "long_110"]
    columns_to_drop = [col for col in added_columns if col not in df_columns]

    return df.drop(*columns_to_drop)


def similar_name_level_within_110_check(
    df: sql.DataFrame, context: OpExecutionContext = None
):
    __test_name__ = "similar name level within 110m"
    logger = get_context_with_fallback_logger(context)
    logger.info(f"Running {__test_name__} checks...")

    df_columns = df.columns

    column_actions = {
        "lat_110": point_110_udf(f.col("latitude")),
        "long_110": point_110_udf(f.col("longitude")),
    }
    df = df.withColumns(column_actions)

    window_spec2 = Window.partitionBy("education_level", "lat_110", "long_110")
    df = df.withColumn(
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        f.when(
            f.count("*").over(window_spec2) > 1,
            1,
        ).otherwise(0),
    )

    df = has_similar_name(df, context)
    df = df.withColumn(
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        f.when(
            f.col("dq_has_similar_name")
            & (f.col("dq_duplicate_similar_name_same_level_within_110m_radius") == 1),
            1,
        ).otherwise(0),
    )

    added_columns = ["lat_110", "long_110", "dq_has_similar_name"]
    columns_to_drop = [col for col in added_columns if col not in df_columns]

    return df.drop(*columns_to_drop)


def school_density_check(df: sql.DataFrame, context: OpExecutionContext = None):
    __test_name__ = "school density"
    logger = ContextLoggerWithLoguruFallback(context, __test_name__)
    logger.log.info(f"Running {__test_name__} checks...")

    column_actions = {
        "latitude": f.col("latitude").cast(FloatType()),
        "longitude": f.col("longitude").cast(FloatType()),
    }
    df = df.withColumns(column_actions)

    df = df.withColumn(
        "hex8",
        h3_geo_to_h3_udf(f.col("latitude"), f.col("longitude")),
    )

    df = df.withColumn(
        "school_density",
        f.count("school_id_giga").over(Window.partitionBy("hex8")),
    )

    df = df.withColumn(
        "dq_is_school_density_greater_than_5",
        f.when(
            f.col("school_density") > 5,
            1,
        ).otherwise(0),
    )

    return df.drop("hex8", "school_density")
