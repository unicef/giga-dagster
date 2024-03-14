import uuid

import h3
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType

from src.settings import settings
from src.spark.config_expectations import config


# STANDARDIZATION FUNCTIONS
def generate_uuid(column_name):
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, str(column_name)))


generate_uuid_udf = f.udf(generate_uuid)


def create_school_id_giga(df):
    school_id_giga_prereqs = ["school_id_govt","school_name","education_level","latitude","longitude"]
    for column in school_id_giga_prereqs:
        if column not in df.columns:
            df = df.withColumn("school_id_giga", f.lit(None))
            return df 
    
    df = df.withColumn(
        "identifier_concat",
        f.concat(
            f.col("school_id_govt"),
            f.col("school_name"),
            f.col("education_level"),
            f.col("latitude"),
            f.col("longitude"),
        ),
    )
    df = df.withColumn("school_id_giga", f.when(
                (f.col("school_id_govt").isNull()) |
                (f.col("school_name").isNull()) |
                (f.col("education_level").isNull()) |
                (f.col("latitude").isNull()) |
                (f.col("longitude").isNull()) 
            , f.lit(None),
        ).otherwise(generate_uuid_udf(f.col("identifier_concat"))))
    
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


def rename_raw_columns(df): ## function for renaming raw files. adhoc
    # Iterate over mapping set and perform actions
    for raw_col, delta_col in config.COLUMN_RENAME_GEOLOCATION:
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


def find_key_by_value(dictionary, value):
    for key, val in dictionary.items():
        if val == value:
            return key
    return None


def column_mapping_rename(df, column_mapping):
    for column in df.columns:
        renamed_column = find_key_by_value(column_mapping, column)
        df = df.withColumnRenamed(f"{column}", f"{renamed_column}")
    return df


def impute_geolocation_columns(df):
    for column in config.GEOLOCATION_COLUMNS:
        if column not in df.columns:
            df = df.withColumn(f"{column}", f.lit(None))
    return df


def bronze_prereq_columns(df):
    df = df.select(*config.GEOLOCATION_COLUMNS)

    return df


# Note: Temporary function for transforming raw files to standardized columns.
def create_bronze_layer_columns(df):
    # Impute missing cols with null
    df = impute_geolocation_columns(df)

    # Select required columns for bronze
    df = bronze_prereq_columns(df)

    # ID
    df = create_school_id_giga(df)  # school_id_giga

    ## Clean up columns -- function shouldnt exist, uploads should be clean
    # df = standardize_internet_speed(df)

    ## Special Cases -- function shouldnt exist, uploads should be clean
    # df = standardize_school_name(df)

    # Timestamp of ingestion
    df = df.withColumn("connectivity_govt_ingestion_timestamp", f.current_timestamp())
    return df


def get_critical_errors_empty_column(*args):
    empty_errors = []

    # Only critical null errors
    for column, value in zip(config.NONEMPTY_COLUMNS_CRITICAL, args, strict=False):
        if value is None:  # If empty (None in PySpark)
            empty_errors.append(column)

    return empty_errors


get_critical_errors_empty_column_udf = f.udf(
    get_critical_errors_empty_column, ArrayType(StringType())
)


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
            "ELSE false END"  # schoolname, lat, long, educ level
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


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    #
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BLZ_school_geolocation_coverage_master.csv"
    # file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/BLZ_master_reference.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"

    
    spark = get_spark_session()
    # master = spark.read.csv(file_url_master, header=True)
    # reference = spark.read.csv(file_url_reference, header=True)
    # df_bronze = master.join(reference, how="left", on="school_id_giga")


    # df = spark.read.csv(file_url, header=True)
    # df = create_bronze_layer_columns(df)
    # df.show() 
    data = [(i, 1) for i in range(10)]

    # Create DataFrame
    df = spark.createDataFrame(data, ["id", "code"])
    df.show()
    columnMapping =  {
      'school_id_govt': 'id',
      'school_name': 'code',
      }
    df = column_mapping_rename(df, columnMapping)
    df = impute_geolocation_columns(df)
    df = create_school_id_giga(df)
    df.show()

    # print(len(df.columns))
    # list_inventory = [
    # "school_id_giga",
    # "school_id_govt",
    # "school_name",
    # "school_establishment_year",
    # "latitude",
    # "longitude",
    # "education_level",
    # "education_level_govt",
    # "connectivity_govt",
    # "connectivity_govt_ingestion_timestamp",
    # "connectivity_govt_collection_year",
    # "download_speed_govt",
    # "download_speed_contracted",
    # "connectivity_type_govt",
    # "admin1",
    # "admin2",
    # "school_area_type",
    # "school_funding_type",
    # "num_computers",
    # "num_computers_desired",
    # "num_teachers",
    # "num_adm_personnel",
    # "num_students",
    # "num_classroom",
    # "num_latrines",
    # "computer_lab",
    # "electricity_availability",
    # "electricity_type",
    # "water_availability",
    # "school_data_source",
    # "school_data_collection_year",
    # "school_data_collection_modality",
    # "school_id_govt_type",
    # "school_address",
    # "is_school_open",
    # "school_location_ingestion_timestamp",
    # ]

    # for col in list_inventory:
    #     if col in df.columns:
    #         print("ok")
    #     else:
    #         print(col)
    # df.show()
    # df = df.limit(10)

    # import json

    # json_file_path =  "src/spark/ABLZ_school-geolocation_gov_20230207_test.json"
    # # # json_file_path =  'C:/Users/RenzTogonon/Downloads/ABLZ_school-geolocation_gov_20230207_test (4).json'

    # with open(json_file_path, 'r') as file:
    #     data = json.load(file)

    # dq_passed_rows(df, data).show()
