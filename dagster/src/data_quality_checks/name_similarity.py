from difflib import SequenceMatcher

from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from dagster import OpExecutionContext
from src.spark.config_expectations import config
from src.utils.logger import get_context_with_fallback_logger

# def has_similar_name(df: sql.DataFrame, context: OpExecutionContext = None):
#     logger = get_context_with_fallback_logger(context)
#     logger.info("Running has similar name checks...")

#     name_list = df.select(f.col("school_name")).collect()
#     name_list = [row["school_name"] for row in name_list]
#     with_similar_name = []

#     for index in range(len(name_list)):
#         string_value = name_list.pop(index)
#         if string_value in with_similar_name:
#             name_list.insert(index, string_value)
#             continue

#         for name in name_list:
#             if (
#                 SequenceMatcher(None, string_value, name).ratio()
#                 > config.SIMILARITY_RATIO_CUTOFF
#             ):
#                 with_similar_name.append(string_value)
#                 with_similar_name.append(name)
#                 break

#         name_list.insert(index, string_value)

#     return df.withColumn(
#         "dq_has_similar_name",
#         f.when(f.col("school_name").isin(with_similar_name), 1).otherwise(0),
#     )


def has_similar_name(df: sql.DataFrame, context: OpExecutionContext = None):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running has similar name checks...")

    name_list = df.select(f.col("school_name")).collect()
    name_list = [row["school_name"] for row in name_list]

    def has_similar_name_check(school_name):
        for name in name_list:
            if (
                school_name != name
                and SequenceMatcher(None, school_name, name).ratio()
                > config.SIMILARITY_RATIO_CUTOFF
            ):
                return name
        return "wala"

    has_similar_name_check_udf = f.udf(has_similar_name_check, StringType())

    return df.withColumn(
        "dq_has_similar_name", has_similar_name_check_udf(f.col("school_name"))
    )


if __name__ == "__main__":
    from src.settings import settings
    from src.utils.spark import get_spark_session

    #
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BRA_school_geolocation_coverage_master.csv"
    # file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/BLZ_master_reference.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"

    spark = get_spark_session()
    master = spark.read.csv(file_url_master, header=True)
    # master.groupBy("admin1").agg(f.count("*").alias("total_count")).orderBy("total_count", ascending=False).show()
    df = master.filter(master["admin1"] == "São Paulo")  # 18.4k rows
    # df = master.filter(master["admin1"] == "Paraíba") #3.8k rows
    # df = master.filter(master["admin1"] == "Sergipe") #1.6k rows
    # df = master.limit(1)
    df = df.select(
        [
            "school_id_giga",
            "school_id_govt",
            "school_name",
            "education_level",
            "latitude",
            "longitude",
        ]
    )
    df.show()
    print(df.count())
    df = has_similar_name(df)
    df.show(1000)

    # df = df.withColumn("latitude", f.lit(6.1671))  # outside boundary <= 150km
    # df = df.withColumn("longitude", f.lit(60.7832)) # outside boundary <= 150km
    # df = df.withColumn("latitude", f.col("latitude").cast("double"))
    # df = df.withColumn("longitude", f.col("longitude").cast("double"))

    # dq_test = is_not_within_country(df, "BRA")
    # dq_test.show()
    # print(dq_test.count())
    # 17 34 for no with_similar_name
