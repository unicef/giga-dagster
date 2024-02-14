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
    CONFIG_PRECISION
)

import decimal
import difflib
import io

# Geospatial
import country_converter as coco
import geopandas as gpd
from geopy.distance import geodesic
from geopy.geocoders import Nominatim

# Spark functions
from pyspark.sql import functions as f
from shapely.geometry import Point
from shapely.ops import nearest_points

# Name Similarity
from thefuzz import fuzz

from azure.storage.blob import BlobServiceClient

# Auth
from src.settings import Settings  # AZURE_SAS_TOKEN, AZURE_BLOB_CONTAINER_NAME
from src.spark.config_expectations import SIMILARITY_RATIO_CUTOFF

# check unique columns (DUPLICATES) (CONFIG_UNIQUE_COLUMNS)
#   "df.filter(F.expr(NOT (count(1) OVER (PARTITION BY giga_id_school unspecifiedframe$()) <= 1)))"
# critical non null columns (CONFIG_NONEMPTY_COLUMNS_CRITICAL)
#   "df.filter(F.expr(NOT (school_name IS NOT NULL)))"
# range checks  CONFIG_VALUES_RANGE
#   "df.filter(F.expr((longitude IS NOT NULL) AND (NOT ((longitude >= -180) AND (longitude <= 180)))))"
# domain checks CONFIG_VALUES_OPTIONS

## CUSTOM EXPECTATIONS ##
# Geospatial (is_within_country)
# not SIMILAR (school name)
# check for five decimal places
# type?? probably not
# unique set columns (CONFIG_UNIQUE_SET_COLUMNS)
#   "df.filter(F.expr(NOT (count(1) OVER (PARTITION BY struct(school_id, school_name, education_level, latitude, longitude)

# Duplicate Checks (dq_duplicate_ config? :hmm:)
def duplicate_checks(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:
        column_name = f"dq_duplicate_{column}"
        df = df.withColumn(
            column_name,
            f.when(
                f.count(f"{column}").over(Window.partitionBy(f"{column}")) > 1,
                1,
            ).otherwise(0),
        )
    return df

def completeness_checks(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:
        column_name = f"dq_isnull_{column}"
        df = df.withColumn(
            column_name,
            f.when(
                f.col(f"{column}").isNull(),
                1,
            ).otherwise(0),
        )
    return df

def range_checks(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:
        df = df.withColumn(
            f"dq_isvalid_{column}",
            f.when(
                f.col(f"{column}").between(
                    CONFIG_COLUMN_LIST[column]["min"],
                    CONFIG_COLUMN_LIST[column]["max"],
                ),
                1,
            ).otherwise(0),
        )
    return df

def domain_checks(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:
        # Note: To isolate: Only school_density and internet_speed_mbps are prioritized among the other value checks
        df = df.withColumn(
            f"dq_isvalid_{column}",
            f.when(
                f.col(f"{column}").isin(
                    CONFIG_COLUMN_LIST[column],
                ),
                1,
            ).otherwise(0),
        )
    return df

# custom checks

## geospatial

settings_instance = Settings()
azure_sas_token = settings_instance.AZURE_SAS_TOKEN
azure_blob_container_name = settings_instance.AZURE_BLOB_CONTAINER_NAME

DUPLICATE_SCHOOL_DISTANCE_KM = 0.1

ACCOUNT_URL = "https://saunigiga.blob.core.windows.net/"
DIRECTORY_LOCATION = "raw/geospatial-data/gadm_files/version4.1/"
container_name = azure_blob_container_name

def get_country_geometry(country_code_iso3):
    try:
        service = BlobServiceClient(account_url=ACCOUNT_URL, credential=azure_sas_token)
        filename = f"{country_code_iso3}.gpkg"
        file = f"{DIRECTORY_LOCATION}{filename}"
        blob_client = service.get_blob_client(container=container_name, blob=file)
        with io.BytesIO() as file_blob:
            download_stream = blob_client.download_blob()
            download_stream.readinto(file_blob)
            file_blob.seek(0)
            gdf_boundaries = gpd.read_file(file_blob)

        country_geometry = gdf_boundaries[gdf_boundaries["GID_0"] == country_code_iso3][
            "geometry"
        ][0]
    except ValueError as e:
        if str(e) == "Must be a coordinate pair or Point":
            country_geometry is None  # noqa: B015
        else:
            raise e

    return country_geometry


def get_point(longitude, latitude):
    try:
        point = Point(longitude, latitude)
    except ValueError as e:
        if str(e) == "Must be a coordinate pair or Point":
            point = None
        else:
            raise e

    return point


def is_within_country(df, country_code_iso3):

    # boundary constants
    geometry = get_country_geometry(country_code_iso3)

    # geopy constants
    country_code_iso2 = coco.convert(names=[country_code_iso3], to="ISO2")


    # Inside based on GADM admin boundaries data
    def is_within_country_gadm(latitude, longitude):
        point = get_point(longitude, latitude)

        if point is not None and geometry is not None:
            return point.within(geometry)

        return None
    

    # Inside based on geopy
    def is_within_country_geopy(latitude, longitude):
        geolocator = Nominatim(user_agent="schools_geolocation")
        coords = f"{latitude},{longitude}"
        location = geolocator.reverse(coords)
    
        if location is None:
            return False
        else:
            geopy_country_code_iso2 = location.raw["address"].get("country_code")
    
            return geopy_country_code_iso2.lower() == country_code_iso2.lower()
        

    # Inside based on boundary distance
    def is_within_boundary_distance(latitude, longitude):
        point = get_point(longitude, latitude)

        if point is not None and geometry is not None:
            p1, p2 = nearest_points(geometry, point)
            point1 = p1.coords[0]
            point2 = (longitude, latitude)
            distance = geodesic(point1, point2).km
            return distance <= 1.5  # km

        return None


    def is_within_country_check(latitude, longitude, country_code_iso3=country_code_iso3):
        if latitude is None or longitude is None or country_code_iso3 is None:
            return False

        is_valid_gadm = is_within_country_gadm(latitude, longitude)
        is_valid_geopy = is_within_country_geopy(latitude, longitude)
        is_valid_boundary = is_within_boundary_distance(latitude, longitude)

        # Note: Check if valid lat/long values
        
        validity = is_valid_gadm | is_valid_geopy | is_valid_boundary

        

        return int(validity)
  
    is_within_country_check_udf = f.udf(is_within_country_check)


    df = df.withColumn(
        "dq_is_within_country",
        is_within_country_check_udf(
            f.col("latitude"), f.col("longitude")
        ))
    
    return df


def has_similar_name(df):
    
    name_list = df.rdd.map(lambda x: x.school_name).collect()

    def similarity_test(column, name_list=name_list):
        for name in name_list:
            if (
                1
                > difflib.SequenceMatcher(None, column, name).ratio()
                >= SIMILARITY_RATIO_CUTOFF
            ):      
                return 1
        return 0
    
    similarity_test_udf = f.udf(similarity_test)

    df = df.withColumn(
        "dq_has_similar_name",
        similarity_test_udf(f.col("school_name")),
    )

    return df


# Decimal places tests

def precision_check(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:
        precision = CONFIG_COLUMN_LIST[column]["min"]
        def get_decimal_places(number, precision=precision):
            if number is None:
                return None
            decimal_places = -decimal.Decimal(str(number)).as_tuple().exponent

            return int(decimal_places >= precision)
        
        get_decimal_places_udf = f.udf(get_decimal_places)
        df = df.withColumn(f"dq_precision_{column}", get_decimal_places_udf(f.col(column)))

    return df


if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    # 
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    df = spark.read.csv(file_url, header=True)
    df = df.sort("school_name").limit(10)
    df = duplicate_checks(df, ["school_id"])
    df = completeness_checks(df, CONFIG_NONEMPTY_COLUMNS_CRITICAL)
    df = range_checks(df, {"latitude": {"min": -85, "max": 90}})
    df = domain_checks(df, {"computer_availability": ["yes", "no"], "electricity_availability": ["yes", "no"]})
    df = is_within_country(df, "BLZ")
    df = has_similar_name(df)
    df = precision_check(df, CONFIG_PRECISION)
    df.show()
    print(CONFIG_PRECISION["latitude"]["min"])

    # ## agg
    # dq_columns = [col for col in df.columns if col.startswith("dq_")]
    # print(dq_columns)
    # df = df.select(*dq_columns)
    # df.show()
    # for column_name in df.columns:
    #     df = df.withColumn(column_name, f.col(column_name).cast("int"))

    # stack_expr = ", ".join([f"'{col.split('_', 1)[1]}', {col}" for col in dq_columns])
    # print(stack_expr)
    
    # unpivoted_df = df.selectExpr(f"stack({len(dq_columns)}, {stack_expr}) as (assertion, value)")
    # unpivoted_df.show()

    # agg_df = unpivoted_df.groupBy("assertion").agg(
    #     f.expr("count(CASE WHEN value = 0 THEN value END) as count_failed"),
    #     f.expr("count(CASE WHEN value = 1 THEN value END) as count_passed"),
    #     f.expr("count(value) as count_overall"),
    #     # f.expr("sum(value) as Sum"),
    #     # f.expr("avg(value) as Average")
    # )

    # agg_df = agg_df.withColumn("percent_failed", (f.col("count_failed")/f.col("count_overall")) * 100)
    # agg_df = agg_df.withColumn("percent_passed", (f.col("count_passed")/f.col("count_overall")) * 100)
    # agg_df.show()


    # json_array = agg_df.toJSON().collect()
    # json_file_path =  "src/spark/test.json"


    # print("JSON array:",json_array)
    
    # # import json

    # # json_file_path =  'C:/Users/RenzTogonon/Downloads/test.json'

    # with open(json_file_path, 'w') as file:
    #     for json_str in json_array:
    #         file.write(json_str + ',\n')

    # with open(json_file_path, 'r') as file:
    #     data = json.load(file)

    # dq_passed_rows(df, data).show()
