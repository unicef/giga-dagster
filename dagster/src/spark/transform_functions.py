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

# STANDARDIZATION FUNCTIONS
def generate_uuid(column_name):
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, str(column_name)))


generate_uuid_udf = f.udf(generate_uuid)


def create_giga_school_id(df):
    df = df.withColumn(
        "identifier_concat",
        f.concat(
            f.col("school_id_gov"),
            f.col("school_name"),
            f.col("education_level"),
            f.col("latitude"),
            f.col("longitude"),
        ),
    )
    df = df.withColumn("school_id_giga", generate_uuid_udf(f.col("identifier_concat")))
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
            "CASE "
            "WHEN district IS NOT NULL AND region IS NOT NULL THEN "
                "CONCAT(school_name, ',', district, ',', region) "
            "WHEN district IS NOT NULL AND city IS NOT NULL THEN "
                "CONCAT(school_name, ',', city, ',', district) "
            "WHEN city IS NOT NULL AND region IS NOT NULL THEN "
                "CONCAT(school_name, ',', city, ',', region) "
            " ELSE CONCAT(COALESCE(school_name, ''), ',', COALESCE(region, ''), ',', COALESCE(region, '')) END"
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
        "download_speed_govt",
        f.expr("regexp_replace(download_speed_govt, '[^0-9.]', '')").cast("float"),
    )
    return df


def h3_geo_to_h3(latitude, longitude):
    if latitude is None or longitude is None:
        return "0"
    else:
        return h3.geo_to_h3(latitude, longitude, resolution=8)


h3_geo_to_h3_udf = f.udf(h3_geo_to_h3)


column_mapping = {
    # raw, delta_col
    ("school_id", "school_id_gov"), 
    ("school_name", "school_name"), 
    ("school_id_gov_type", "school_id_gov_type"), 
    ("school_establishment_year", "school_establishment_year"), 
    ("latitude", "latitude"), 
    ("longitude", "longitude"), 
    ("education_level", "education_level"), 
    ("education_level_govt", "education_level_govt"), 
    ("internet_availability", "connectivity_govt"), 
    ("connectivity_govt_ingestion_timestamp", "connectivity_govt_ingestion_timestamp"),
    ("internet_speed_mbps", "download_speed_govt"), 
    ("download_speed_contracted", "download_speed_contracted"), 
    ("internet_type", "connectivity_type_govt"), 
    ("admin1", "admin1"), 
    ("admin2", "admin2"), 
    ("school_region", "school_area_type"), 
    ("school_funding_type", "school_funding_type"),
    ("computer_count", "num_computers"), 
    ("desired_computer_count", "num_computers_desired"), 
    ("teacher_count", "num_teachers"), 
    ("adm_personnel_count", "num_adm_personnel"), 
    ("student_count", "num_students"), 
    ("classroom_count", "num_classroom"), 
    ("num_latrines", "num_latrines"), 
    ("computer_lab", "computer_lab"), 
    ("electricity", "electricity_availability"), 
    ("electricity_type", "electricity_type"), 
    ("water", "water_availability"), 
    ("address", "school_address"), 
    ("school_data_source", "school_data_source"), 
    ("school_data_collection_year", "school_data_collection_year"), 
    ("school_data_collection_modality", "school_data_collection_modality"),
    ("is_open", "is_school_open"),
    }


def rename_raw_columns(df):
    # Iterate over mapping set and perform actions
    for raw_col, delta_col in column_mapping:
    # Check if the raw column exists in the DataFrame
        if raw_col in df.columns:
        # If it exists in raw, rename it to the delta column
            df = df.withColumnRenamed(raw_col, delta_col)
        # If it doesn't exist in both, create a null column placeholder with the delta column name
        elif delta_col in df.columns:
            pass
        else:
            df = df.withColumn(delta_col, f.lit(None))
    
    df = bronze_prereq_columns(df)

    return df


def bronze_prereq_columns(df):
    bronze_prereq_columns = [delta_col for _, delta_col in column_mapping]
    df = df.select(*bronze_prereq_columns)
    
    return df 


# Note: Temporary function for transforming raw files to standardized columns.
def create_bronze_layer_columns(df):
    # Select required columns for bronze
    df = bronze_prereq_columns(df)
    
    # ID
    df = create_giga_school_id(df)

    # School Density Computation
    df = df.withColumn("latitude", df["latitude"].cast("float"))
    df = df.withColumn("longitude", df["longitude"].cast("float"))
    df = df.withColumn("hex8", h3_geo_to_h3_udf(f.col("latitude"), f.col("longitude")))
    df = df.withColumn(
        "school_density", f.count("school_id_giga").over(Window.partitionBy("hex8"))
    )

    # Clean up columns
    df = standardize_internet_speed(df)

    # Special Cases
    # df = standardize_school_name(df)

    # Temp key for index
    w = Window().orderBy(f.lit("A"))
    df = df.withColumn("gx_index", f.row_number().over(w))

    # Timestamp of ingestion
    df = df.withColumn("school_location_ingestion_timestamp", f.current_timestamp())
    
    # df = df.withColumn("country_code", f.lit("BLZ"))
    # df = df.withColumn(
    #     "is_within_country",
    #     is_within_country_udf(
    #         f.col("latitude"), f.col("longitude"), f.col("country_code")
    #         )
    #     )
    

    return df


def get_critical_errors_empty_column(*args):
    empty_errors = []

    # Only critical null errors
    for column, value in zip(CONFIG_NONEMPTY_COLUMNS_CRITICAL, args, strict=False):
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
        column_name = f"duplicate_{column}"
        df = df.withColumn(
            column_name,
            f.when(
                f.count(f"{column}").over(Window.partitionBy(f"{column}")) > 1,
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
            "   OR duplicate_school_id_giga = true "
            "   OR size(critical_error_empty_column) > 0 "  # schoolname, lat, long, educ level
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
        column_name = f"missing_{column}_flag"
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
            f"duplicate_{set_name}",
            f.when(f.count("*").over(Window.partitionBy(column_set)) > 1, 1).otherwise(
                0
            ),
        )

    for value in CONFIG_VALUES_RANGE_PRIO:
        # Note: To isolate: Only school_density and internet_speed_mbps are prioritized among the other value checks
        df = df.withColumn(
            f"is_valid_{value}",
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


def critical_error_indices_json_parse(data):
    key_id = list(data["run_results"].keys())[0]
    nest_results = data["run_results"][key_id]["validation_result"]["results"]
    index = list(range(len([x for x in nest_results])))

    ## critical error tags ##
    for i in index:
        expectation_config = nest_results[i]["expectation_config"]
        result = nest_results[i]["result"]

    # duplicate school_id "expect_column_values_to_be_unique"
        if expectation_config["expectation_type"] == "expect_column_values_to_be_unique" and expectation_config["kwargs"]["column"] == "school_id_gov":
            data = result["unexpected_index_list"]
            index_list = [entry['gx_index'] for entry in data]
            index_dup_school_id_set = set(index_list)

    # duplicate school_id_giga "expect_column_values_to_be_unique"
        elif expectation_config["expectation_type"] == "expect_column_values_to_be_unique" and expectation_config["kwargs"]["column"] == "school_id_giga":
            data = result["unexpected_index_list"]
            index_list = [entry['gx_index'] for entry in data]
            index_dup_gid_set = set(index_list)

    # empty school_name "expect_column_values_to_not_be_null"
        elif expectation_config["expectation_type"] == "expect_column_values_to_not_be_null" and expectation_config["kwargs"]["column"] == "school_name":
            data = result["unexpected_index_list"]
            index_list = [entry['gx_index'] for entry in data]
            index_null_school_name_set = set(index_list)

    # empty latitude "expect_column_values_to_not_be_null"
        elif expectation_config["expectation_type"] == "expect_column_values_to_not_be_null" and expectation_config["kwargs"]["column"] == "latitude":
            data = result["unexpected_index_list"]
            index_list = [entry['gx_index'] for entry in data]
            index_null_lat_set = set(index_list)

    # empty longitude "expect_column_values_to_not_be_null"
        elif expectation_config["expectation_type"] == "expect_column_values_to_not_be_null" and expectation_config["kwargs"]["column"] == "longitude":
            data = result["unexpected_index_list"]
            index_list = [entry['gx_index'] for entry in data]
            index_null_long_set = set(index_list)

    # empty education_level "expect_column_values_to_not_be_null"
        elif expectation_config["expectation_type"] == "expect_column_values_to_not_be_null" and expectation_config["kwargs"]["column"] == "education_level":
            data = result["unexpected_index_list"]
            index_list = [entry['gx_index'] for entry in data]
            index_null_educ_level_set = set(index_list)

    # valid location values latitude "expect_column_values_to_be_between"
        elif expectation_config["expectation_type"] == "expect_column_values_to_be_between" and expectation_config["kwargs"]["column"] == "latitude":
            data = result["unexpected_index_list"]
            index_list = [entry['gx_index'] for entry in data]
            index_invalid_lat_set = set(index_list)

    # valid location values longitude "expect_column_values_to_be_between"
        elif expectation_config["expectation_type"] == "expect_column_values_to_be_between" and expectation_config["kwargs"]["column"] == "longitude":
            data = result["unexpected_index_list"]
            index_list = [entry['gx_index'] for entry in data]
            index_invalid_long_set = set(index_list)

    # # within country is_within_country "add_"expect_column_values_to_be_in_set"
    #     elif expectation_config["expectation_type"] == "expect_column_values_to_be_unique" and expectation_config["kwargs"]["column"] == "giga_id_school":
    #         print(i)
    #         data = result["unexpected_index_list"]
    #         schema = StructType([StructField("gx_index", StringType(), True)])
    #         df_unique_giga_id_school = spark.createDataFrame(data, schema=schema)

    critical_error_indices_string = list(index_dup_school_id_set | index_dup_gid_set | index_null_school_name_set | index_null_lat_set | index_null_long_set | index_null_educ_level_set | index_invalid_lat_set | index_invalid_long_set)
    critical_error_indices = [int(x) for x in critical_error_indices_string]

    return critical_error_indices

def dq_passed_rows(bronze_df, json_results):
    critical_error_indices = critical_error_indices_json_parse(json_results)
    df = bronze_df.filter(f.col("gx_index").isin(critical_error_indices))

    return df
    
def dq_failed_rows(bronze_df, json_results):
    critical_error_indices = critical_error_indices_json_parse(json_results)
    df = bronze_df.filter(~f.col("gx_index").isin(critical_error_indices))

    return df

if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    # 
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    df = spark.read.csv(file_url, header=True)
    df = rename_raw_columns(df)
    df = create_bronze_layer_columns(df)
    df.sort("school_name").limit(10).show()
    # df.show()
    # df = df.limit(10)
    
    
    # import json

    # json_file_path =  "src/spark/ABLZ_school-geolocation_gov_20230207_test.json"
    # # # json_file_path =  'C:/Users/RenzTogonon/Downloads/ABLZ_school-geolocation_gov_20230207_test (4).json'

    # with open(json_file_path, 'r') as file:
    #     data = json.load(file)

    # dq_passed_rows(df, data).show()
