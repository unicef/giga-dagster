import os
import uuid

import h3 
import pandas as pd
from src.spark.check_functions import (
    duplicate_check,
    get_decimal_places,
    # get_decimal_places_udf,
    has_no_similar_name_fuzz,
    is_same_name_level_within_radius,
    is_similar_name_level_within_radius,
    is_valid_range,
    is_within_country,
    is_within_country_udf,
    is_within_boundary_distance_udf
)
from src.spark.config_expectations import (
    CONFIG_NONEMPTY_COLUMNS_CRITICAL,
    CONFIG_NONEMPTY_COLUMNS_WARNING,
    CONFIG_UNIQUE_COLUMNS,
    CONFIG_UNIQUE_SET_COLUMNS,
    CONFIG_VALUES_RANGE,
)

# AZURE_SAS_TOKEN = os.environ.get("AZURE_SAS_TOKEN")

from .spark import get_spark_session
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, BooleanType, ArrayType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window


file_url = "wasbs://giga-dataops-dev@saunigiga.blob.core.windows.net/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
spark = get_spark_session()
df_spark = spark.read.csv(file_url, header=True)

# STANDARDIZATION FUNCTIONS

def generate_uuid(column_name):
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, str(column_name)))

generate_uuid_udf = f.udf(generate_uuid)

def create_giga_school_id(df):
    df = df.withColumn(
            "identifier_concat", 
                f.concat(
                    f.col("school_id"),
                    f.col("school_name"),
                    f.col("education_level"),
                    f.col("latitude"),
                    f.col("longitude")
                    )
                )
    df = df.withColumn("giga_id_school", generate_uuid_udf(f.col("identifier_concat")))
    return df.drop("identifier_concat")

def create_uzbekistan_school_name(df):
    school_name_col = "school_name"
    district_col = "district"
    city_col = "city"
    region_col = "region"

    # spark doesnt have a function like pd.notna, checking first for column existence
    if school_name_col not in df.columns:
        df = df.withColumn(school_name_col, f.lit(None).cast("string"))
    elif district_col not in df.columns:
        df = df.withColumn(district_col, f.lit(None).cast("string"))
    elif city_col not in df.columns:
        df = df.withColumn(city_col, f.lit(None).cast("string"))
    elif region_col not in df.columns:
        df = df.withColumn(region_col, f.lit(None).cast("string"))
    else:
        pass

    # case when expr for concats
    df = df.withColumn("school_name",
            f.expr(
                "CASE "
                    "WHEN district IS NOT NULL AND region IS NOT NULL THEN CONCAT(school_name, ',', district, ',', region) "
                    "WHEN district IS NOT NULL AND city IS NOT NULL THEN CONCAT(school_name, ',', city, ',', district) "
                    "WHEN city IS NOT NULL AND region IS NOT NULL THEN CONCAT(school_name, ',', city, ',', region) "
                    "ELSE CONCAT(COALESCE(school_name, ''), ',', COALESCE(region, ''), ',', COALESCE(region, '')) END"
                )
            )

    return df

def standardize_school_name(df):
    # filter
    df1 = df.filter(df.country_code == "UZB")
    df2 = df.filter(df.country_code != "UZB")
    
    # uzb transform
    df1 = create_uzbekistan_school_name(df1)
    df = df2.union(df1)

    return df

def standardize_internet_speed(df):
    df= df.withColumn("internet_speed_mbps", f.expr("regexp_replace(internet_speed_mbps, '[^0-9.]', '')").cast("float"))
    return df

def h3_geo_to_h3(latitude, longitude): 
    if latitude is None or longitude is None:
        return "0"
    else:
        return h3.geo_to_h3(latitude, longitude, resolution = 8)

h3_geo_to_h3_udf = f.udf(h3_geo_to_h3) 

# def has_duplicate(row):
#     return (
#         row["duplicate_id_name_level_location"]
#         or row["duplicate_level_location"]
#         or row["has_similar_name"]
#         or row["duplicate_name_level_within_110m_radius"]
#         or row["duplicate_similar_name_same_level_within_100m_radius"]
#     )


# Note: Temporary function for transforming raw files to standardized columns.
# This should eventually be converted to dbt transformations.
def create_bronze_layer_columns(df):
    # ID
    df = create_giga_school_id(df) 
    
    # School Density Computation
    df = df.withColumn("latitude", df["latitude"].cast("float"))
    df = df.withColumn("longitude", df["longitude"].cast("float"))
    df = df.withColumn("hex8", h3_geo_to_h3_udf(f.col("latitude"), f.col('longitude')))
    df = df.withColumn("school_density", f.count("giga_id_school").over(Window.partitionBy("hex8")))

    # Clean up columns
    df = standardize_internet_speed(df)

    # Special Cases
    # df = standardize_school_name(df)

    return df

def get_critical_errors_empty_column(*args):
    empty_errors = []

    # Only critical null errors
    for column, value in zip(CONFIG_NONEMPTY_COLUMNS_CRITICAL, args):
        if value is None:  # If empty (None in PySpark)
            empty_errors.append(column)

    return empty_errors

get_critical_errors_empty_column_udf = f.udf(get_critical_errors_empty_column, ArrayType(StringType()))

def create_error_columns(df, country_code_iso3):
    # 1. School name should not be null
    # 2. Latitude should not be null.
    # 3. Longitude should not be null
    df = df.withColumn("critical_error_empty_column", 
                       get_critical_errors_empty_column_udf(*[f.col(column) for column in CONFIG_NONEMPTY_COLUMNS_CRITICAL])
                       )

    # 4. School id should be unique
    # 5. Giga School ID should be unique
    for column in CONFIG_UNIQUE_COLUMNS:
        column_name = "duplicate_{}".format(column)
        df = df.withColumn(column_name, 
                            f.when(
                                f.count("{}".format(column)).over(Window.partitionBy("{}".format(column))) > 1, True
                            ).otherwise(False)
                        )

    # 6. School latitude and longitude should be valid values
    df = df.withColumn("is_valid_location_values", 
                        f.when(
                            f.col("latitude").between(CONFIG_VALUES_RANGE["latitude"]["min"], CONFIG_VALUES_RANGE["latitude"]["max"])
                            & f.col("longitude").between(CONFIG_VALUES_RANGE["longitude"]["min"], CONFIG_VALUES_RANGE["longitude"]["max"])
                            , True).otherwise(False)
                       )

    # 7. School latitude and longitude should be in the expected country
    df = df.withColumn("country_code", f.lit(country_code_iso3))
    # df = df.withColumn("is_within_country", is_within_boundary_distance_udf(f.col("latitude"), f.col("longitude"), f.col("country_code")))
    # df["is_within_country"] = df.apply(
    #     lambda row: is_within_country(
    #         row["latitude"], row["longitude"], country_code_iso3
    #     )
    #     if row["is_valid_location_values"] is True
    #     else False,
    #     axis="columns",
    # )

    return df

    

def has_critical_error(df):
    # Check if there is any critical error flagged for the row
    df = df.withColumn("has_critical_error",
            f.expr(
                "CASE "
                    "WHEN duplicate_school_id = true "
                    "   OR duplicate_giga_id_school = true "
                    "   OR size(critical_error_empty_column) > 0 "
                    "   OR is_valid_location_values = false "
                    # "   OR is_within_country != true "
                    "   THEN true "
                    "ELSE false END"
                )
            )
    # # Outside country
    # if row["is_within_country"] is not True:
    #     return True

    return df


# def is_within_range(value, min, max):
#     if pd.isna(value) is False:
#         return value >= min and value <= max
#     return None

import decimal
def get_decimal_places(number):
    decimal_places = -decimal.Decimal(str(number)).as_tuple().exponent

    return decimal_places

get_decimal_places_udf = f.udf(get_decimal_places)

def create_staging_layer_columns(df):
    df = df.withColumn("precision_longitude", get_decimal_places_udf(f.col("longitude")))
    df = df.withColumn("precision_latitude", get_decimal_places_udf(f.col("latitude")))

    for column in CONFIG_NONEMPTY_COLUMNS_WARNING:
        column_name = "missing_{}_flag".format(column)
        df = df.withColumn(column_name, f.when(f.col(column).isNull(), True).otherwise(False))

#     for column_set in CONFIG_UNIQUE_SET_COLUMNS:
#         set_name = "_".join(column_set)
#         df["duplicate_{}".format(set_name)] = df.duplicated(
#             subset=column_set, keep=False
#         )

#     for value in CONFIG_VALUES_RANGE:
#         # Note: To isolate: Only school_density and internet_speed_mbps are prioritized among the other value checks
#         df["is_valid_{}".format(value)] = df[value].apply(
#             lambda x: is_within_range(
#                 x,
#                 CONFIG_VALUES_RANGE[value]["min"],
#                 CONFIG_VALUES_RANGE[value]["max"],
#             )
#         )

#     # Custom checks
#     df["has_similar_name"] = not has_no_similar_name_fuzz(df["school_name"])
#     df["duplicate_name_level_within_110m_radius"] = duplicate_check(
#         df, is_same_name_level_within_radius
#     )
#     df["duplicate_similar_name_same_level_within_100m_radius"] = duplicate_check(
#         df, is_similar_name_level_within_radius
#     )
#     df["duplicate"] = df.apply(has_duplicate, axis="columns")

    return df


if __name__ == "__main__":
    # file_url = "https://@saunigiga.dfs.core.windows.net/giga-dataops-dev/bronze/school-geolocation-data/BLZ-school_geolocation-20230207.csv"
    # df = pd.read_csv(
        # "abfs://@saunigiga.dfs.core.windows.net/giga-dataops-dev/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv",
        # storage_options={"sas_token": AZURE_SAS_TOKEN},
    # )
    # filepath = (
    #     "./great_expectations/plugins/expectations/BLZ-school_geolocation-20230207.csv"
    # )
    # df = pd.read_csv(filepath)

    # Standardize columns
    # print("Standardizing for Bronze Layer")
    # df_bronze = create_bronze_layer_columns(df)
    # df_bronze.to_csv("bronze_school_geolocation.csv")

    # Perform data quality checks for identifying critical errors (dropping those rows)
    # print("Creating Staging Check Columns")
    # To Do: Merge bronze + silver to create the complete staging data before running duplicate checks
    # df_staging = create_error_columns(df_bronze, "BLZ")
    # df_staging["has_critical_error"] = df.apply(
        # lambda x: has_critical_error(x), axis="columns"
    # )
    # df_staging.to_csv("staging_flags.csv")

    # Gather critical errors into error report
    # df_critical_errors = df_staging[df_staging["has_critical_error"]]
    # df_critical_errors.to_csv("critical_errors.csv")

    # Filter out critical errors for clean staging data
    # df_staging_clean = df_staging[~df_staging["has_critical_error"]]
    # df_staging_clean.to_csv("staging_clean.csv")

    # testing
    # test_columns = ["school_id", "school_name", "education_level", "latitude", "longitude", "internet_speed_mbps"]
    test_columns_latitude = ["latitude", "longitude", "mobile_internet_generation","internet_availability","internet_type","internet_speed_mbps"]
    df = df_spark.select(test_columns_latitude)
    # df = df.limit(10)
    # df = create_bronze_layer_columns(df)
    # df = df.withColumn("country_code", f.lit("BLZ"))
    # df = df.withColumn("is_within_country", is_within_boundary_distance_udf(f.col("latitude"), f.col("longitude"), f.col("country_code")))
    # df = create_error_columns(df)
    # df.show()
    # # df.orderBy("school_name").show()
    # data = [("School1", "District2", "Region1", "School", 181, 2),
    #         ("School2", None, None, "School", 8, -2),
    #         ("School1", "District2", "Region3", "School", 8, -222)]
    # columns = ["school_id", "giga_id_school", "education_level", "school_name", "latitude", "longitude"]
    # df = spark.createDataFrame(data, columns)
    # df = create_bronze_layer_columns(df)
    # df = create_error_columns(df, "BLZ")
    # df = has_critical_error(df)
    # print(CONFIG_NONEMPTY_COLUMNS_CRITICAL)
    # df = df.withColumn("precision_longitude", get_decimal_places_udf(f.col("longitude")))
    df = create_staging_layer_columns(df)
    df.show(10, truncate=False)
    # df.filter(df["school_name"] == "Hill Top Primary School").show()
    # df.filter(df["has_critical_error"] == True).show(30)
    # print(is_within_country(1,1,"BLZ"))
