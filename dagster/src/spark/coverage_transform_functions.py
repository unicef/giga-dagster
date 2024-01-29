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
)

## facebook

# transform percent_-g column from raw to boolean

def percent_to_boolean(df):
    df = df.withColumn("2G_coverage", f.col("percent_2G") > 0)
    df = df.withColumn("3G_coverage", f.col("percent_3G") > 0)
    df = df.withColumn("4G_coverage", f.col("percent_4G") > 0)
    
    df = df.drop("percent_2G")
    df = df.drop("percent_3G")
    df = df.drop("percent_4G")
    return df

# standardize functions

def itu_lower_columns(df):
    # to do: config
    itu_cols_to_rename = ['Schools_within_1km', 'Schools_within_2km', 'Schools_within_3km', 'Schools_within_10km']
    for col_name in itu_cols_to_rename:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df

fb_cols_to_keep = ['giga_id_school', '2G_coverage', '3G_coverage', '4G_coverage']
itu_cols_to_keep = ['giga_id_school', '2G_coverage', '3G_coverage', 'fiber_node_distance',
                        'microwave_node_distance', 'nearest_school_distance', 'schools_within_1km', 'schools_within_2km',
                        'schools_within_3km', 'schools_within_10km', 'nearest_LTE_id', 'nearest_LTE_distance',
                        'nearest_UMTS_id', 'nearest_UMTS_distance', 'nearest_GSM_id', 'nearest_GSM_distance',
                        '2G_coverage', '3G_coverage', '4G_coverage', 'pop_within_1km', 'pop_within_2km',
                        'pop_within_3km', 'pop_within_10km']

# def itu_columns_to_keep(df):


if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    # 
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/raw/school_geolocation_coverage_data/bronze/coverage_data/UZB_school-coverage_meta_20230927-091814.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    df = spark.read.csv(file_url, header=True)
    # df = rename_raw_columns(df)
    # df = create_bronze_layer_columns(df)
    # df.sort("school_name").limit(10).show()
    df = percent_to_boolean(df)
    df.show()

