import uuid

import h3
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

    # Temp key for index
    w = Window().orderBy(f.lit('A'))
    df = df.withColumn("temp_pk", f.row_number().over(w))
    
    df = df.withColumn("country_code", f.lit("BLZ"))
    df = df.withColumn(
        "is_within_country",
        is_within_country_udf(
            f.col("latitude"), f.col("longitude"), f.col("country_code")
            ),
        )

    bronze_columns = [
        "temp_pk", 
        "school_id" ,"school_name" ,"education_level" ,
        "latitude" ,"longitude" ,"student_count" ,
        "internet_availability" ,"internet_type" ,"mobile_internet_generation" ,
        "internet_speed_mbps" ,"electricity_availability" ,"computer_availability" ,
        "computer_count" ,"school_year", "hex8", "giga_id_school", "school_density" ,
        "is_within_country"
    ]
    df = df.select(*bronze_columns)

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
                1,
            ).otherwise(0),
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
            1,
        ).otherwise(0),
    )

    # 7. School latitude and longitude should be in the expected country
    df = df.withColumn("country_code", f.lit(country_code_iso3))
    df = df.withColumn(
        "is_within_country",
        is_within_country_udf(
            f.col("latitude"), f.col("longitude"), f.col("country_code")
        ),
    )

    return df


def has_critical_error(df):
    # Check if there is any critical error flagged for the row
    df = df.withColumn(
        "has_critical_error",
        f.expr(
            "CASE "
            "WHEN duplicate_school_id = true "
            "   OR duplicate_giga_id_school = true "
            "   OR size(critical_error_empty_column) > 0 " #schoolname, lat, long, educ level
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
    df = df.withColumn(
        "precision_longitude", get_decimal_places_udf(f.col("longitude"))
    )
    df = df.withColumn("precision_latitude", get_decimal_places_udf(f.col("latitude")))

    for column in CONFIG_NONEMPTY_COLUMNS_WARNING:
        column_name = "missing_{}_flag".format(column)
        df = df.withColumn(column_name, f.when(f.col(column).isNull(), 1).otherwise(0))

    df = df.withColumn(
        "location_id",
        f.concat_ws(
            "_", f.col("longitude").cast("string"), f.col("latitude").cast("string")
        ),
    )

    for column_set in CONFIG_UNIQUE_SET_COLUMNS:
        set_name = "_".join(column_set)
        df = df.withColumn(
            "duplicate_{}".format(set_name),
            f.when(f.count("*").over(Window.partitionBy(column_set)) > 1, 1).otherwise(
                0
            ),
        )

    for value in CONFIG_VALUES_RANGE_PRIO:
        # Note: To isolate: Only school_density and internet_speed_mbps are prioritized among the other value checks
        df = df.withColumn(
            "is_valid_{}".format(value),
            f.when(
                f.col("latitude").between(
                    CONFIG_VALUES_RANGE[value]["min"],
                    CONFIG_VALUES_RANGE[value]["max"],
                ),
                1,
            ).otherwise(0),
        )

    # Custom checks
    name_list = df.rdd.map(lambda x: x.school_name).collect()
    df = df.withColumn("school_name_list", f.lit(name_list))
    df = df.withColumn(
        "has_similar_name",
        has_similar_name_udf(f.col("school_name"), f.col("school_name_list")),
    )
    df = df.drop("school_name_list")

    # duplicate_name_level_within_110m_radius
    # #     df["duplicate_name_level_within_110m_radius"] = duplicate_check(
    # #         df, is_same_name_level_within_radius

    df = df.withColumn("lat_110", point_110_udf(f.col("latitude")))
    df = df.withColumn("long_110", point_110_udf(f.col("longitude")))
    window_spec1 = Window.partitionBy(
        "school_name", "education_level", "lat_110", "long_110"
    )
    df = df.withColumn(
        "duplicate_name_level_within_110m_radius",
        f.when(f.count("*").over(window_spec1) > 1, 1).otherwise(0),
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
        ),
    )

    # df = df.withColumn("coordinates", f.array("latitude", "longitude"))
    # df = df.withColumn("coordinates_list", f.collect_list("coordinates").over(window_spec))
    # df = df.withColumn("duplicate_name_level_within_110m_radius", are_all_points_beyond_minimum_distance_udf(f.col("coordinates_list")))

    # duplicate_similar_name_same_level_within_100m_radius
    #     df["duplicate_similar_name_same_level_within_100m_radius"] = duplicate_check(
    #         df, is_similar_name_level_within_radius

    window_spec2 = Window.partitionBy("education_level", "lat_110", "long_110")
    df = df.withColumn(
        "duplicate_similar_name_same_level_within_100m_radius",
        f.when(f.count("*").over(window_spec2) > 1, 1).otherwise(0),
    )
    df = df.withColumn(
        "duplicate_similar_name_same_level_within_100m_radius",
        f.expr(
            "CASE "
            "   WHEN has_similar_name = true "
            "       AND duplicate_similar_name_same_level_within_100m_radius = 1 "
            "       THEN 1 "
            "ELSE 0 END"
        ),
    )

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
        ),
    )

    return df


if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    # 
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    # schema = StructType([
    # StructField("school_id", StringType(), True),
    # StructField("school_name", StringType(), True),
    # StructField("education_level", StringType(), True),
    # StructField("latitude", StringType(), True),
    # StructField("longitude", StringType(), True),
    # StructField("student_count", StringType(), True),
    # StructField("internet_availability", StringType(), True),
    # StructField("internet_type", StringType(), True),
    # StructField("mobile_internet_generation", StringType(), True),
    # StructField("internet_speed_mbps", StringType(), True),
    # StructField("electricity_availability", StringType(), True),
    # StructField("computer_availability", StringType(), True),
    # StructField("computer_count", StringType(), True),
    # StructField("school_year", StringType(), True),
    # # Add more fields as needed
    # ])
    df = spark.read.csv(file_url, header=True)
    # df.show()
    # df = df.limit(10)
    # df = create_bronze_layer_columns(df)
    # df.show()
    
    
    import json

    json_file_path =  'C:/Users/RenzTogonon/Downloads/ABLZ_school-geolocation_gov_20230207_test (4).json'

    with open(json_file_path, 'r') as file:
        data = json.load(file)


    # def dq_passed_rows():
        # dynamic result identifier
        nest_results = data["run_results"]["ValidationResultIdentifier::expectation_school_geolocation/__none__/20231206T004054.099698Z/ab369d75cdfccd42dcc290d1dfd27171"]["validation_result"]["results"]
        index = list(range(len([x for x in nest_results])))

        for i in index:
            if nest_results[i]["expectation_config"]["expectation_type"] == "expect_column_values_to_be_unique" and nest_results[i]["expectation_config"]["kwargs"]["column"] == "school_id":
                print(i)
                print(nest_results[i]["result"]["unexpected_index_list"])
            else: 
                print("ngek")



        ## critical error tags ##
        # duplicate school_id "expect_column_values_to_be_unique"
        # duplicate giga_id_school "expect_column_values_to_be_unique"
        # empty school_name  "expect_column_values_to_not_be_null"
        # empty latitude "expect_column_values_to_not_be_null"
        # empty longitude "expect_column_values_to_not_be_null"
        # empty education_level "expect_column_values_to_not_be_null"
        # valid location values latitude longitude "expect_column_values_to_be_between"
        # within country is_within_country "add_"expect_column_values_to_be_in_set"
