from pyspark import sql
from pyspark.sql import (
    Window,
    functions as f,
)
from pyspark.sql.types import FloatType

from dagster import OpExecutionContext
from src.spark.user_defined_functions import (
    find_similar_names_in_group_udf,
    h3_geo_to_h3_udf,
)
from src.utils.logger import (
    ContextLoggerWithLoguruFallback,
    get_context_with_fallback_logger,
)


def duplicate_name_level_110_check(
    df: sql.DataFrame,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate level within 110m checks...")

    df_columns = df.columns

    df = df.withColumn("lat_110", f.floor(f.col("latitude") * 1000) / 1000)
    df = df.withColumn("long_110", f.floor(f.col("longitude") * 1000) / 1000)
    window_spec1 = Window.partitionBy(
        "school_name",
        "education_level",
        "lat_110",
        "long_110",
    )
    df = df.withColumn(
        "dq_duplicate_name_level_within_110m_radius",
        f.when(f.count("*").over(window_spec1) > 1, 1).otherwise(0),
    )

    added_columns = ["lat_110", "long_110"]
    columns_to_drop = [col for col in added_columns if col not in df_columns]

    df = df.withColumn(
        "dq_duplicate_name_level_within_110m_radius",
        f.when(
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("latitude")),
            f.lit(None).cast("int"),
        ).otherwise(f.col("dq_duplicate_name_level_within_110m_radius")),
    )

    return df.drop(*columns_to_drop)


def similar_name_level_within_110_check(
    df: sql.DataFrame,
    context: OpExecutionContext = None,
):
    __test_name__ = "similar name level within 110m"
    logger = get_context_with_fallback_logger(context)
    logger.info(f"Running {__test_name__} checks...")

    df_columns = df.columns

    # Generate Lat 110, Long 110
    column_actions = {
        "lat_110": f.floor(f.col("latitude") * 1000) / 1000,
        "long_110": f.floor(f.col("longitude") * 1000) / 1000,
    }
    df = df.withColumns(column_actions)

    # Get duplicates across education level, lat 110, long 110
    window_spec2 = Window.partitionBy("education_level", "lat_110", "long_110")
    df = df.withColumn(
        "duplicate_level_within_110m_radius",
        f.when(
            f.count("*").over(window_spec2) > 1,
            1,
        ).otherwise(0),
    )

    # Group school names by location and education level for schools with duplicates
    # IMPORTANT: Filter out invalid coordinates (NULL, NaN, or 0,0) BEFORE grouping
    # to prevent O(N²) explosion when many schools have missing coordinates
    valid_coords_filter = (
        f.col("latitude").isNotNull()
        & f.col("longitude").isNotNull()
        & ~f.isnan(f.col("latitude"))
        & ~f.isnan(f.col("longitude"))
        & ~((f.col("latitude") == 0) & (f.col("longitude") == 0))
    )

    school_names_grouped = (
        df.filter((df["duplicate_level_within_110m_radius"] == 1) & valid_coords_filter)
        .groupBy("education_level", "lat_110", "long_110")
        .agg(f.collect_list("school_name").alias("school_names"))
    )

    # Find similar names within each group using vectorized comparison
    school_names_grouped = school_names_grouped.withColumn(
        "similar_names_list", find_similar_names_in_group_udf(f.col("school_names"))
    )

    # Explode the list of similar names to get individual rows
    df_similar = school_names_grouped.select(
        "education_level",
        "lat_110",
        "long_110",
        f.explode("similar_names_list").alias("school_name_similar"),
    )

    # Join to original dataset and tag entries with similar names
    # Use aliases to avoid duplicate column names from the join
    df = df.alias("left_df").join(
        df_similar.alias("right_df"),
        (f.col("left_df.school_name") == f.col("right_df.school_name_similar"))
        & (f.col("left_df.education_level") == f.col("right_df.education_level"))
        & (f.col("left_df.lat_110") == f.col("right_df.lat_110"))
        & (f.col("left_df.long_110") == f.col("right_df.long_110")),
        how="left",
    )

    # Select only the left side columns plus school_name_similar from right side
    # This avoids duplicate column names that would cause ambiguous references later
    columns_to_select = [f.col(f"left_df.{col}") for col in df_columns]
    columns_to_select.append(f.col("right_df.school_name_similar"))
    df = df.select(*columns_to_select)

    df = df.withColumn(
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        f.when(f.col("school_name_similar").isNull(), 0).otherwise(1),
    )

    df = df.withColumn(
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        f.when(
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("latitude")),
            f.lit(None).cast("int"),
        ).otherwise(f.col("dq_duplicate_similar_name_same_level_within_110m_radius")),
    )

    added_columns = [
        "lat_110",
        "long_110",
        "duplicate_level_within_110m_radius",
        "school_name_similar",
    ]
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
            # only set to 1 if lat/lon exist
            f.col("school_density") > 5,
            1,
        ).otherwise(0),
    )

    df = df.withColumn(
        "dq_is_school_density_greater_than_5",
        f.when(
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("latitude")),
            f.lit(None).cast("int"),
        ).otherwise(f.col("dq_is_school_density_greater_than_5")),
    )

    return df.drop("hex8", "school_density")


if __name__ == "__main__":
    from datetime import datetime

    from src.settings import settings
    from src.utils.spark import get_spark_session

    current_time = datetime.now()

    #
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BRA_school_geolocation_coverage_master.csv"
    # file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/BLZ_master_reference.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"

    spark = get_spark_session()
    df = spark.read.csv(file_url_master, header=True)
    # df.groupBy("admin1").agg(f.count("*").alias("total_count")).orderBy("total_count", ascending=False).show()
    # df = df.filter(df["admin1"] == "São Paulo")  # 18.4k rows
    # df = df.filter(df["admin1"] == "Paraíba") #3.8k rows
    # df = df.filter(df["admin1"] == "Sergipe") #1.6k rows
    # df = df.limit(1)
    df = df.select(
        [
            "school_id_giga",
            "school_id_govt",
            "school_name",
            "education_level",
            "latitude",
            "longitude",
        ],
    )
    df.show()
    print(df.count())
    df = similar_name_level_within_110_check(df)
    # df.orderBy(f.desc('duplicate_level_within_110m_radius'), "lat_110").show(100, truncate=False)
    df.show()
    print(df.count())

    # school_names = df.select(f.col("school_name")).filter(df["dq_duplicate_similar_name_same_level_within_110m_radius"] == 1).distinct()
    # print(school_names.count())
    # distinct_names = school_names.select(f.col("school_name").alias("distinct_names")).distinct()
    # df_l = school_names.crossJoin(distinct_names)

    # df_l = df_l.withColumn(
    #     "dq_has_similar_name", has_similar_name_check_udf(f.col("school_name"), f.col("distinct_names"))
    # )
    # df_l.show()

    # df_l = df_l.filter(df_l["dq_has_similar_name"] == 1)
    # df_l.show(20)
    # print(df_l.count())

    # df_l = df_l.select(f.col("school_name")).distinct()
    # df_l.show(20)
    # print(df_l.count())

    # df = df.drop("distinct_names")
    # df.show(20)

    # current_partitions = df.rdd.getNumPartitions()
    # print("Current number of partitions:", current_partitions)

    # df = df.withColumn("latitude", f.lit(6.1671))  # outside boundary <= 150km
    # df = df.withColumn("longitude", f.lit(60.7832)) # outside boundary <= 150km
    # df = df.withColumn("latitude", f.col("latitude").cast("double"))
    # df = df.withColumn("longitude", f.col("longitude").cast("double"))

    # dq_test = is_not_within_country(df, "BRA")
    # dq_test.show()
    # print(dq_test.count())
    # 17 34 for no with_similar_name
