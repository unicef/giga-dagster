import uuid

import h3
import json
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, IntegerType
from pyspark.sql.window import Window

from src.settings import settings
from src.spark.check_functions import (
    get_decimal_places_udf,
    has_similar_name_udf,
    is_within_country_udf,
)
from src.spark.config_expectations import (
    CONFIG_NONEMPTY_COLUMNS_MASTER,
    CONFIG_UNIQUE_COLUMNS_MASTER,
    CONFIG_UNIQUE_COLUMNS_GEOLOCATION,
    CONFIG_UNIQUE_COLUMNS_COVERAGE,
    CONFIG_UNIQUE_SET_COLUMNS,
    CONFIG_VALUES_DOMAIN_MASTER,
    CONFIG_VALUES_RANGE_PRIO,
    CONFIG_PRECISION,
    CONFIG_VALUES_RANGE_COVERAGE,
    CONFIG_VALUES_RANGE_MASTER,
    CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_MASTER,
    CONFIG_UNIQUE_COLUMNS_CRITICAL,
    CONFIG_NONEMPTY_COLUMNS_CRITICAL,
    CONFIG_VALUES_RANGE_CRITICAL,
    CONFIG_DATA_QUALITY_CHECKS_DESCRIPTIONS,
    CONFIG_VALUES_DOMAIN_ALL,
    CONFIG_VALUES_RANGE_ALL,
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



# STANDARD CHECKS
def duplicate_checks(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:

        column_name = f"dq_duplicate-{column}"
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

        if column in df.columns:
            column_name = f"dq_isnull-{column}"
            df = df.withColumn(
                column_name,
                f.when(
                    f.col(f"{column}").isNull(),
                    1,
                ).otherwise(0),
            )
        else:
            df = df.withColumn(f"dq_isnull-{column}", f.lit(1))
    return df


def range_checks(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:

        if column in df.columns:
            df = df.withColumn(
                f"dq_isinvalidrange-{column}",
                f.when(
                    f.col(f"{column}").between(
                        CONFIG_COLUMN_LIST[column]["min"],
                        CONFIG_COLUMN_LIST[column]["max"],
                    ),
                    0,
                ).otherwise(1),
            )
        else:
            df = df.withColumn(f"dq_isinvalidrange-{column}", f.lit(1))
    return df


def domain_checks(df, CONFIG_COLUMN_LIST):
    for column in CONFIG_COLUMN_LIST:

        if column in df.columns:
            if None in CONFIG_COLUMN_LIST[column]:
                df = df.withColumn(
                    f"dq_isinvaliddomain-{column}",
                    f.when(
                        (f.col(f"{column}").isNull()) |
                        (f.col(f"{column}").isin(CONFIG_COLUMN_LIST[column])),
                        0,
                    ).otherwise(1),
                )
            else:
                df = df.withColumn(
                    f"dq_isinvaliddomain-{column}",
                    f.when(
                        f.col(f"{column}").isin(
                            CONFIG_COLUMN_LIST[column],
                        ),
                        0,
                    ).otherwise(1),
                )
        else:
            df = df.withColumn(f"dq_isinvaliddomain-{column}", f.lit(1))
    return df


# CUSTOM CHECKS
# Within Country Check

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


def is_not_within_country(df, country_code_iso3):

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


    def is_not_within_country_check(latitude, longitude, country_code_iso3=country_code_iso3):
        if latitude is None or longitude is None or country_code_iso3 is None:
            return False

        is_valid_gadm = is_within_country_gadm(latitude, longitude)
        is_valid_geopy = is_within_country_geopy(latitude, longitude)
        is_valid_boundary = is_within_boundary_distance(latitude, longitude)

        validity = is_valid_gadm | is_valid_geopy | is_valid_boundary

        return int(not validity)
  
    is_not_within_country_check_udf = f.udf(is_not_within_country_check)


    df = df.withColumn(
        "dq_is_not_within_country",
        is_not_within_country_check_udf(
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

            return int(decimal_places < precision)
        
        get_decimal_places_udf = f.udf(get_decimal_places)
        df = df.withColumn(f"dq_precision-{column}", get_decimal_places_udf(f.col(column)))

    return df

# Duplicate Sets checks

def duplicate_set_checks(df, CONFIG_COLUMN_LIST):
    df = df.withColumn(
        "location_id",
        f.concat_ws(
            "_", f.col("longitude").cast("string"), f.col("latitude").cast("string")
        ),
    )
    for column_set in CONFIG_COLUMN_LIST:

        set_name = "_".join(column_set)
        df = df.withColumn(
            f"dq_duplicateset-{set_name}",
            f.when(f.count("*").over(Window.partitionBy(column_set)) > 1, 1).otherwise(
                0
            ),
        )
    df = df.drop("location_id")
    
    return df

def duplicate_all_except_checks(df, CONFIG_COLUMN_LIST):
    for column_set in CONFIG_COLUMN_LIST:

        df = df.withColumn(
            f"dq_duplicate_all_except_school_code",
            f.when(f.count("*").over(Window.partitionBy(column_set)) > 1, 1).otherwise(
                0
            ),
        )

    return df

# Geospatial Checks

def point_110(column):
    if column is None:
        return None
    point = int(1000 * float(column)) / 1000
    return point


point_110_udf = f.udf(point_110)


def duplicate_name_level_110_check(df):
    df_columns = df.columns

    df = df.withColumn("lat_110", point_110_udf(f.col("latitude")))
    df = df.withColumn("long_110", point_110_udf(f.col("longitude")))
    window_spec1 = Window.partitionBy(
        "school_name", "education_level", "lat_110", "long_110"
    )
    df = df.withColumn(
        "dq_duplicate_name_level_within_110m_radius",
        f.when(f.count("*").over(window_spec1) > 1, 1).otherwise(0),
    )

    added_columns = ["lat_110", "long_110"]
    for col in added_columns: # if the check existed in the column before applying this check, dont drop
        
        if col in df_columns:
            pass
        else: 
            df = df.drop(col)

    return df


def similar_name_level_within_110_check(df):
    df_columns = df.columns
    
    df = df.withColumn("lat_110", point_110_udf(f.col("latitude")))
    df = df.withColumn("long_110", point_110_udf(f.col("longitude")))
    window_spec2 = Window.partitionBy("education_level", "lat_110", "long_110")

    df = df.withColumn(
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        f.when(f.count("*").over(window_spec2) > 1, 1).otherwise(0),
    )
    df = has_similar_name(df)
    df = df.withColumn(
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        f.expr(
            "CASE "
            "   WHEN dq_has_similar_name = true "
            "       AND dq_duplicate_similar_name_same_level_within_110m_radius = 1 "
            "       THEN 1 "
            "ELSE 0 END"
        ),
    )

    added_columns = ["lat_110", "long_110", "dq_has_similar_name"]

    for col in added_columns: # if the check existed in the column before applying this check, dont drop
        
        if col in df_columns:
            pass
        else: 
            df = df.drop(col)

    return df


def h3_geo_to_h3(latitude, longitude):
    if latitude is None or longitude is None:
        return "0"
    else:
        return h3.geo_to_h3(latitude, longitude, resolution=8)


h3_geo_to_h3_udf = f.udf(h3_geo_to_h3)


def school_density_check(df):
    df = df.withColumn("latitude", df["latitude"].cast("float"))
    df = df.withColumn("longitude", df["longitude"].cast("float"))
    df = df.withColumn("hex8", h3_geo_to_h3_udf(f.col("latitude"), f.col("longitude")))
    df = df.withColumn(
        "school_density", f.count("school_id_giga").over(Window.partitionBy("hex8"))
    )
    df = df.withColumn(
        "dq_is_school_density_greater_than_5",
            f.when(f.col("school_density") > 5, 1).otherwise(0),
        )
    df = df.drop("hex8")
    df = df.drop("school_density")

    return df


# Critical Error Check

def critical_error_checks(df, country_code_iso3):
    df = duplicate_checks(df, CONFIG_UNIQUE_COLUMNS_CRITICAL)
    df = completeness_checks(df, CONFIG_NONEMPTY_COLUMNS_CRITICAL)
    df = range_checks(df, CONFIG_VALUES_RANGE_CRITICAL)
    df = is_not_within_country(df, country_code_iso3)
    df = df.withColumn(
        "dq_has_critical_error",
        f.expr(
            "CASE "
            "WHEN `dq_duplicate-school_id_govt` "
            "   + `dq_duplicate-school_id_giga` "
            "   + `dq_isnull-school_name` "
            "   + `dq_isnull-latitude` "
            "   + `dq_isnull-longitude` "
            "   + `dq_isinvalidrange-latitude` "
            "   + `dq_isinvalidrange-longitude` "
            "   + `dq_is_not_within_country` > 0"
            "   THEN 1 "
            "ELSE 0 END"
        ),
    )

    return df




# Outputs

def aggregate_report_sparkdf(df, CONFIG_DATA_QUALITY_CHECKS_DESCRIPTIONS=CONFIG_DATA_QUALITY_CHECKS_DESCRIPTIONS):
    dq_columns = [col for col in df.columns if col.startswith("dq_")]
    
    df = df.select(*dq_columns)

    for column_name in df.columns:
        df = df.withColumn(column_name, f.col(column_name).cast("int"))

    # unpivot row level checks
    stack_expr = ", ".join([f"'{col.split('_', 1)[1]}', `{col}`" for col in dq_columns])
    unpivoted_df = df.selectExpr(f"stack({len(dq_columns)}, {stack_expr}) as (assertion, value)")
    # unpivoted_df.show()

    agg_df = unpivoted_df.groupBy("assertion").agg(
        f.expr("count(CASE WHEN value = 1 THEN value END) as count_failed"),
        f.expr("count(CASE WHEN value = 0 THEN value END) as count_passed"),
        f.expr("count(value) as count_overall"),
    )

    agg_df = agg_df.withColumn("percent_failed", (f.col("count_failed")/f.col("count_overall")) * 100)
    agg_df = agg_df.withColumn("percent_passed", (f.col("count_passed")/f.col("count_overall")) * 100)

    ## processing for human readable report
    agg_df = agg_df.withColumn("column", (f.split(f.col("assertion"), "-").getItem(1)))
    agg_df = agg_df.withColumn("assertion", (f.split(f.col("assertion"), "-").getItem(0)))
    # agg_df.show()

    # descriptions
    configs_df = spark.createDataFrame(CONFIG_DATA_QUALITY_CHECKS_DESCRIPTIONS)
    # configs_df.show(truncate=False)

    
    # range
    r_rows = [(key, value["min"], value.get("max")) for key, value in CONFIG_VALUES_RANGE_ALL.items()]
    range_schema = StructType([
        StructField("column", StringType(), True),
        StructField("min", IntegerType(), True),
        StructField("max", IntegerType(), True)  
    ])  
    range_df = spark.createDataFrame(r_rows, schema=range_schema)
    # range_df.show(truncate=False)

    # domain
    d_rows = [(key, value) for key, value in CONFIG_VALUES_DOMAIN_ALL.items()]
    domain_schema = StructType([
        StructField("column", StringType(), True),
        StructField("set", ArrayType(StringType(), True), True)
    ]) 
    domain_df = spark.createDataFrame(d_rows, schema=domain_schema)
    # domain_df.show(truncate=False)

    # report construction
    report = agg_df.join(configs_df, "assertion", "left")
    report = report.join(range_df, "column", "left")
    report = report.join(domain_df, "column", "left")
    report = report.withColumn("description", 
                               f.when(f.col("column").isNull(), f.col("description"))
                               .otherwise(f.regexp_replace("description", "\\{\\}", f.col("column"))))
    report = report.withColumn("description", 
                               f.when(f.col("min").isNull(), f.col("description"))
                               .otherwise(f.regexp_replace("description", "\\{min\\}", f.col("min"))))
    report = report.withColumn("description", 
                               f.when(f.col("max").isNull(), f.col("description"))
                               .otherwise(f.regexp_replace("description", "\\{max\\}", f.col("max"))))
    report = report.withColumn("description", 
                               f.when(f.col("set").isNull(), f.col("description"))
                               .otherwise(f.regexp_replace("description", "\\{set\\}", f.array_join(f.col("set"), ", "))))
    report = report.select(
        "assertion",
        "column",
        "description",
        "count_failed",
        "count_passed",
        "count_overall",
        "percent_failed",
        "percent_passed",
    )

    return report

def aggregate_report_json(df):
    json_array = df.toJSON().collect()
    json_file_path =  "src/spark/test.json"
    
    import json


    print("JSON array:",json_array)
    with open(json_file_path, 'w') as file:
        for json_str in json_array:
            file.write(json_str + ',\n')
    return json_array
    


if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    # 
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BLZ_school_geolocation_coverage_master.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    spark = get_spark_session()
    df = spark.read.csv(file_url, header=True)
    df = df.sort("school_name").limit(10)
    df = df.withColumnRenamed("school_id_gov", "school_id_govt")
    df = duplicate_checks(df, CONFIG_UNIQUE_COLUMNS_MASTER)
    df = completeness_checks(df, CONFIG_NONEMPTY_COLUMNS_MASTER)
    df = domain_checks(df, CONFIG_VALUES_DOMAIN_MASTER)
    df = range_checks(df, CONFIG_VALUES_RANGE_MASTER)
    df = precision_check(df, CONFIG_PRECISION)
    df = is_not_within_country(df, "BLZ")
    df = duplicate_set_checks(df, CONFIG_UNIQUE_SET_COLUMNS)
    df = duplicate_all_except_checks(df, CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_MASTER)
    df = duplicate_name_level_110_check(df)
    df = similar_name_level_within_110_check(df)
    # df = critical_error_checks(df, "BLZ")
    df = school_density_check(df)


    df.show()
    df = aggregate_report_sparkdf(df)
    df.show()

    # print(CONFIG_VALUES_DOMAIN_ALL)
    # print(CONFIG_VALUES_RANGE_ALL)

    
    

    # df.write.csv("src/spark/test.csv", header=True)

    # df = domain_checks(df, {"computer_availability": ["yes", "no"], "electricity_availability": ["yes", "no"]})
    # df = has_similar_name(df)

            

    # json_file_path =  'C:/Users/RenzTogonon/Downloads/test.json'
    # with open(json_file_path, 'r') as file:
    #     data = json.load(file)

    # dq_passed_rows(df, data).show()
