import uuid

import h3
import json
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.settings import settings
from src.spark.check_functions import (
    get_decimal_places_udf,
    has_similar_name_udf,
    is_within_country_udf,
)
from src.spark.config_expectations import (
    CONFIG_NONEMPTY_COLUMNS_CRITICAL,
    CONFIG_NONEMPTY_COLUMNS_WARNING,
    CONFIG_UNIQUE_COLUMNS,
    CONFIG_UNIQUE_SET_COLUMNS,
    CONFIG_VALUES_RANGE,
    CONFIG_VALUES_RANGE_PRIO,
    CONFIG_ITU_COLUMNS_TO_RENAME,
    CONFIG_FB_COLUMNS,
    CONFIG_ITU_COLUMNS,
)

## facebook

# transform percent_-g column from raw to boolean

def fb_percent_to_boolean(df):
    df = df.withColumn("2G_coverage", f.col("percent_2G") > 0)
    df = df.withColumn("3G_coverage", f.col("percent_3G") > 0)
    df = df.withColumn("4G_coverage", f.col("percent_4G") > 0)
    
    df = df.drop("percent_2G")
    df = df.drop("percent_3G")
    df = df.drop("percent_4G")
    return df

def itu_binary_to_boolean(df):
    df = df.withColumn("2G_coverage", f.col("2G") >= 1)
    df = df.withColumn("3G_coverage", f.col("3G") == 1)
    df = df.withColumn("4G_coverage", f.col("4G") == 1)
    
    df = df.drop("2G")
    df = df.drop("3G")
    df = df.drop("4G")
    return df

# standardize functions

def itu_lower_columns(df):
    for col_name in CONFIG_ITU_COLUMNS_TO_RENAME:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df

def coverage_column_filter(df, CONFIG_COLUMNS_TO_KEEP):
    df = df.select(*CONFIG_COLUMNS_TO_KEEP)
    return df

def coverage_row_filter(df):
    df = df.filter(f.col("giga_id_school").isNotNull())
    return df

def coverage_pipeline(fb, itu):

    # fb
    fb = fb_percent_to_boolean(fb)
    fb = coverage_column_filter(fb, CONFIG_FB_COLUMNS)
    fb = coverage_row_filter(fb)

    # itu
    itu = itu_binary_to_boolean(itu)
    itu = itu_lower_columns(itu)
    itu = coverage_column_filter(itu)
    itu = coverage_row_filter(itu)

    # add suffixes
    for col in itu.columns:
        if col != 'giga_id_school' and col in fb.columns:
            itu = itu.withColumnRenamed(col, col + '_itu')

    coverage_df = fb.join(itu, on="giga_id_school", how="outer")

    return coverage_df





if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    # 
    file_url_fb = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/bronze/coverage_data/UZB_school-coverage_meta_20230927-091814.csv"
    file_url_itu = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/bronze/coverage_data/UZB_school-coverage_itu_20230927-091823.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    fb = spark.read.csv(file_url_fb, header=True)
    itu = spark.read.csv(file_url_itu, header=True)
    # df = rename_raw_columns(df)
    # df = create_bronze_layer_columns(df)
    # df.sort("school_name").limit(10).show()
    # df = itu_lower_columns(df)
    # df = fb_percent_to_boolean(df)
    # df = column_filter(df, CONFIG_FB_COLUMNS_TO_KEEP)
    df = coverage_pipeline(fb, itu)
    df.show()
