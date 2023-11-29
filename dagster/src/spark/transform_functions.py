import os
import uuid

import h3
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, BooleanType, StringType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

from src.spark.check_functions import (
    duplicate_check,
    get_decimal_places,
    get_decimal_places_udf,
    is_same_name_level_within_radius,
    is_similar_name_level_within_radius,
    is_valid_range,
    is_within_boundary_distance_udf,
    is_within_country,
    is_within_country_udf,
    has_similar_name_udf,
)
from src.spark.config_expectations import (
    CONFIG_NONEMPTY_COLUMNS_CRITICAL,
    CONFIG_NONEMPTY_COLUMNS_WARNING,
    CONFIG_UNIQUE_COLUMNS,
    CONFIG_UNIQUE_SET_COLUMNS,
    CONFIG_VALUES_RANGE,
    CONFIG_VALUES_RANGE_PRIO,
)

from src._utils.spark import get_spark_session

# make this dynamic
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
            f.col("longitude"),
        ),
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
    df = df.withColumn(
        "school_name",
        f.expr(
            "CASE WHEN district IS NOT NULL AND region IS NOT NULL THEN"
            " CONCAT(school_name, ',', district, ',', region) WHEN district IS NOT NULL"
            " AND city IS NOT NULL THEN CONCAT(school_name, ',', city, ',', district)"
            " WHEN city IS NOT NULL AND region IS NOT NULL THEN CONCAT(school_name,"
            " ',', city, ',', region) ELSE CONCAT(COALESCE(school_name, ''), ',',"
            " COALESCE(region, ''), ',', COALESCE(region, '')) END"
        ),
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
    df = df.withColumn(
        "internet_speed_mbps",
            f.expr("regexp_replace(internet_speed_mbps, '[^0-9.]', '')").cast("float"),
    )
    return df


def h3_geo_to_h3(latitude, longitude):
    if latitude is None or longitude is None:
        return "0"
    else:
        return h3.geo_to_h3(latitude, longitude, resolution=8)

h3_geo_to_h3_udf = f.udf(h3_geo_to_h3)


# Note: Temporary function for transforming raw files to standardized columns.
# This should eventually be converted to dbt transformations.
def create_bronze_layer_columns(df):
    # ID
    df = create_giga_school_id(df)

    # School Density Computation
    df = df.withColumn("latitude", df["latitude"].cast("float"))
    df = df.withColumn("longitude", df["longitude"].cast("float"))
    df = df.withColumn("hex8", h3_geo_to_h3_udf(f.col("latitude"), f.col("longitude")))
    df = df.withColumn(
        "school_density", f.count("giga_id_school").over(Window.partitionBy("hex8"))
    )

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


get_critical_errors_empty_column_udf = f.udf(
    get_critical_errors_empty_column, ArrayType(StringType())
)


def create_error_columns(df, country_code_iso3):
    # 1. School name should not be null
    # 2. Latitude should not be null.
    # 3. Longitude should not be null
    df = df.withColumn(
        "critical_error_empty_column",
        get_critical_errors_empty_column_udf(
            *[f.col(column) for column in CONFIG_NONEMPTY_COLUMNS_CRITICAL]
        ),
    )

    # 4. School id should be unique
    # 5. Giga School ID should be unique
    for column in CONFIG_UNIQUE_COLUMNS:
        column_name = "duplicate_{}".format(column)
        df = df.withColumn(
            column_name,
            f.when(
                f.count("{}".format(column)).over(
                    Window.partitionBy("{}".format(column))
                )
                > 1,
                1
            ).otherwise(0)
        )

    # 6. School latitude and longitude should be valid values
    df = df.withColumn(
        "is_valid_location_values",
        f.when(
            f.col("latitude").between(
                CONFIG_VALUES_RANGE["latitude"]["min"],
                CONFIG_VALUES_RANGE["latitude"]["max"],
            )
            & f.col("longitude").between(
                CONFIG_VALUES_RANGE["longitude"]["min"],
                CONFIG_VALUES_RANGE["longitude"]["max"],
            ),
            1
        ).otherwise(0)
    )

    # 7. School latitude and longitude should be in the expected country
    df = df.withColumn("country_code", f.lit(country_code_iso3))
    df = df.withColumn("is_within_country", is_within_country_udf(f.col("latitude"), f.col("longitude"), f.col("country_code")))

    return df


def has_critical_error(df):
    # Check if there is any critical error flagged for the row
    df = df.withColumn(
        "has_critical_error",
        f.expr(
            "CASE "
            "WHEN duplicate_school_id = true "
            "   OR duplicate_giga_id_school = true "
            "   OR size(critical_error_empty_column) > 0 "
            "   OR is_valid_location_values = false "
            "   OR is_within_country != true "
            "   THEN true "
            "ELSE false END"
        ),
    )

    return df


def coordinates_comp(coordinates_list, row_coords):
    # coordinates_list = [coords for coords in coordinates_list if coords != row_coords]
    for sublist in coordinates_list:
        if sublist == row_coords:
            coordinates_list.remove(sublist)
            break
    return coordinates_list 

coordinates_comp_udf = f.udf(coordinates_comp)


def point_110(column):
    if column is None:
        return None
    point = int(1000 * float(column)) / 1000
    return point 

point_110_udf = f.udf(point_110)


def create_staging_layer_columns(df):
    df = df.withColumn("precision_longitude", get_decimal_places_udf(f.col("longitude")))
    df = df.withColumn("precision_latitude", get_decimal_places_udf(f.col("latitude")))

    for column in CONFIG_NONEMPTY_COLUMNS_WARNING:
        column_name = "missing_{}_flag".format(column)
        df = df.withColumn(
            column_name, f.when(f.col(column).isNull(), 1).otherwise(0)
        )
    
    df = df.withColumn("location_id", f.concat_ws("_", f.col('longitude').cast('string'), f.col('latitude').cast('string')))

    for column_set in CONFIG_UNIQUE_SET_COLUMNS:
        set_name = "_".join(column_set)
        df = df.withColumn(
            "duplicate_{}".format(set_name), 
            f.when(
                f.count("*").over(Window.partitionBy(column_set))
                    > 1, 1).otherwise(0)
                )

    for value in CONFIG_VALUES_RANGE_PRIO:
    # Note: To isolate: Only school_density and internet_speed_mbps are prioritized among the other value checks
        df = df.withColumn(
            "is_valid_{}".format(value),
            f.when(
                f.col("latitude").between(
                    CONFIG_VALUES_RANGE[value]["min"],
                    CONFIG_VALUES_RANGE[value]["max"],
                ), 1).otherwise(0)
        )
    

    # Custom checks
    name_list = df.rdd.map(lambda x: x.school_name).collect()
    df = df.withColumn("school_name_list", f.lit(name_list))
    df = df.withColumn("has_similar_name", has_similar_name_udf(f.col("school_name"), f.col("school_name_list")))
    df = df.drop("school_name_list")

    # duplicate_name_level_within_110m_radius
    # #     df["duplicate_name_level_within_110m_radius"] = duplicate_check(
    # #         df, is_same_name_level_within_radius

    df = df.withColumn("lat_110", point_110_udf(f.col("latitude")))    
    df = df.withColumn("long_110", point_110_udf(f.col("longitude")))    
    window_spec1 = Window.partitionBy("school_name", "education_level", "lat_110", "long_110")
    df = df.withColumn(
        "duplicate_name_level_within_110m_radius", 
            f.when(
                f.count("*").over(window_spec1)
                    > 1, 1).otherwise(0)
                )
    df = df.withColumn(
        "duplicate_name_level_within_110m_radius",
        f.expr(
            "CASE "
            "   WHEN duplicate_school_id_school_name_education_level_location_id = 1 "
            "       OR duplicate_school_name_education_level_location_id = 1 "
            "       OR duplicate_education_level_location_id = 1 "
            "       THEN 0 "
            "ELSE duplicate_name_level_within_110m_radius END"
        ))

    # df = df.withColumn("coordinates", f.array("latitude", "longitude"))
    # df = df.withColumn("coordinates_list", f.collect_list("coordinates").over(window_spec))
    # df = df.withColumn("duplicate_name_level_within_110m_radius", are_all_points_beyond_minimum_distance_udf(f.col("coordinates_list")))

    # duplicate_similar_name_same_level_within_100m_radius
    #     df["duplicate_similar_name_same_level_within_100m_radius"] = duplicate_check(
    #         df, is_similar_name_level_within_radius

    window_spec2 = Window.partitionBy("education_level", "lat_110", "long_110")
    df = df.withColumn(
        "duplicate_similar_name_same_level_within_100m_radius", 
            f.when(
                f.count("*").over(window_spec2)
                    > 1, 1).otherwise(0)
                )
    df = df.withColumn(
        "duplicate_similar_name_same_level_within_100m_radius",
        f.expr(
            "CASE "
            "   WHEN has_similar_name = true "
            "       AND duplicate_similar_name_same_level_within_100m_radius = 1 "
            "       THEN 1 "
            "ELSE 0 END"
        ))

    #     df["duplicate"] = df.apply(has_duplicate, axis="columns")
    df = df.withColumn(
        "duplicate",
        f.expr(
            "CASE "
            "   WHEN duplicate_school_id_school_name_education_level_location_id = 1 "
            "       OR duplicate_school_name_education_level_location_id = 1 "
            "       OR duplicate_education_level_location_id = 1 "
            "       OR duplicate_name_level_within_110m_radius = 1 "
            "       OR duplicate_similar_name_same_level_within_100m_radius = 1 "
            "       THEN 1 "
            "ELSE 0 END"
        ))

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
    # test_columns_latitude = [
    #     "latitude",
    #     "longitude",
    #     "mobile_internet_generation",
    #     "internet_availability",
    #     "internet_type",
    #     "internet_speed_mbps",
    # ]
    # test_columns_unique_set = [
    #     "school_id", "school_name", "education_level", "latitude", "longitude", "internet_speed_mbps"]
    # df = df_spark.select(test_columns_unique_set)
    # df = create_bronze_layer_columns(df)
    
    
#     data = [
#     ("School1", "District2", 90, 180),
#     ("School2", None, 8, -2),
#     ("School1", "District2", 90, 180),
#     ("School3", "ewna", 8, -2),
#     ("School1", "District2", 89, 180),
# ]
#     columns = ["school_name", "education_level", "latitude", "longitude"]
#     df = spark.createDataFrame(data, columns)
    # df = df.withColumn("latitude", df["latitude"].cast("float"))
    # df = df.withColumn("longitude", df["longitude"].cast("float"))

    # df = df.withColumn("coordinates", f.array("latitude", "longitude"))

    # window_spec = Window.partitionBy("school_name", "education_level")
    # df = df.withColumn("coordinates_list", f.collect_list("coordinates").over(window_spec))


    # df = df.withColumn("coordinates_list", coordinates_comp_udf(f.col("coordinates_list"), f.col("coordinates")))
    # df = df.withColumn("duplicate_name_level_within_110m_radius", are_all_points_beyond_minimum_distance_udf(f.col("coordinates_list")))
    # df.show(truncate=False)


    # name_list = df.rdd.map(lambda x: x.school_name).collect()
    # df = df.withColumn("school_name_list", f.lit(name_list))
    # df = df.withColumn("has_similar_name", has_no_similar_name_udf(f.col("school_name"), f.col("school_name_list")))
    # df.orderBy(f.desc("has_similar_name")).show(400)

    # df.show(truncate = False)
    # name_list = df.rdd.map(lambda x: x.internet_speed_mbps).collect()
    # print(len(name_list))
    # print(range(len(name_list)))
    # print([True for i in range(len(name_list))])


    # for value in CONFIG_VALUES_RANGE_PRIO:
    # # Note: To isolate: Only school_density and internet_speed_mbps are prioritized among the other value checks
    #     df = df.withColumn(
    #         "is_valid_{}".format(value),
    #         f.when(
    #             f.col(value).between(
    #                 CONFIG_VALUES_RANGE[value]["min"],
    #                 CONFIG_VALUES_RANGE[value]["max"],
    #             ), True).otherwise(False)
    #     )
    # df.orderBy("school_density").show()
    # print(CONFIG_VALUES_RANGE["internet_speed_mbps"]["min"])
    # print(CONFIG_VALUES_RANGE["internet_speed_mbps"]["max"])
    # df = df.limit(10)
    # df = create_error_columns(df, "BLZ")
    # df.show()
    # df = df.withColumn("country_code", f.lit("BLZ"))
    # df = df.withColumn("is_within_country", is_within_boundary_distance_udf(f.col("latitude"), f.col("longitude"), f.col("country_code")))
    # # df.orderBy("school_name").show()
    # data = [("School1", "District2", "Region1", "School", 181, 2),
    #         ("School2", None, None, "School", 8, -2),
    #         ("School1", "District2", "Region3", "School", 8, -222)]
    # columns = ["school_id", "giga_id_school", "education_level", "school_name", "latitude", "longitude"]
    # df = create_error_columns(df, "BLZ")
    # df = has_critical_error(df)
    # print(CONFIG_NONEMPTY_COLUMNS_CRITICAL)
    # df = df.withColumn("precision_longitude", get_decimal_places_udf(f.col("longitude")))
    # df = create_staging_layer_columns(df)
    # df = df.withColumn("country_code", f.lit("BLZ"))
    # df = df.withColumn("is_within_country", is_within_country_udf(f.col("latitude"), f.col("longitude"), f.col("country_code")))
    # df.show(10, truncate=False)
    # df = df.filter(df["school_name"] == "NAZARENE PRIMARY SCHOOL")
    # df.filter(df["has_critical_error"] == True).show(30)
    # print(is_within_country(1,1,"BLZ"))
    
        # df = df.withColumn(
        #     column_name,
        #     f.when(
        #         f.count("{}".format(column)).over(
        #             Window.partitionBy("{}".format(column))
        #         )
        #         > 1,
        #         True,
        #     ).otherwise(False),
        # )
    # for value in CONFIG_VALUES_RANGE['internet_speed_mbps']:
    #     print(value)
    df = df_spark
    # df = create_staging_layer_columns(df)
    df = create_bronze_layer_columns(df)
    # df = df.withColumn("precision_longitude", get_decimal_places_udf(f.col("longitude")))
    df = create_error_columns(df, "BLZ")
    # df = df.withColumn("country_code", f.lit("BLZ"))
    # df = df.withColumn("is_within_country", is_within_boundary_distance_udf(f.col("latitude"), f.col("longitude"), f.col("country_code")))
    df.show()
    # print(is_within_country(16.99555, -88.3668, "BLZ"))
    # df = has_critical_error(df)
    # df = df.withColumn("lat_110", point_110_udf(f.col("latitude")))    
    # df = df.withColumn("long_110", point_110_udf(f.col("longitude")))    
    
    # duplicate_name_level_within_110m_radius
    
    # df.filter(df["duplicate"] == 1).show()
    # df.filter(df["school_name"] == "Yo Creek Sacred Heart RC Primary School").show()
