import io
import uuid

import geopandas as gpd
import h3
from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, NullType, StringType, StructField

from azure.storage.blob import BlobServiceClient
from src.settings import settings
from src.spark.config_expectations import config
from src.spark.udf_dependencies import get_point

ACCOUNT_URL = "https://saunigiga.blob.core.windows.net/"
azure_sas_token = settings.AZURE_SAS_TOKEN
azure_blob_container_name = settings.AZURE_BLOB_CONTAINER_NAME
container_name = azure_blob_container_name


# STANDARDIZATION FUNCTIONS
def generate_uuid(identifier_concat: str):
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, str(identifier_concat)))


generate_uuid_udf = f.udf(generate_uuid)


def create_school_id_giga(df: sql.DataFrame):
    school_id_giga_prereqs = [
        "school_id_govt",
        "school_name",
        "education_level",
        "latitude",
        "longitude",
    ]
    for column in school_id_giga_prereqs:
        if column not in df.columns:
            df = df.withColumn("school_id_giga", f.lit(None))
            return df

    df = df.withColumn(
        "identifier_concat",
        f.concat(
            f.col("school_id_govt").cast(StringType()),
            f.col("school_name").cast(StringType()),
            f.col("education_level").cast(StringType()),
            f.col("latitude").cast(StringType()),
            f.col("longitude").cast(StringType()),
        ),
    )
    df = df.withColumn(
        "school_id_giga",
        f.when(
            (f.col("school_id_govt").isNull())
            | (f.col("school_name").isNull())
            | (f.col("education_level").isNull())
            | (f.col("latitude").isNull())
            | (f.col("longitude").isNull()),
            f.lit(None),
        ).otherwise(generate_uuid_udf(f.col("identifier_concat"))),
    )

    return df.drop("identifier_concat")


def create_uzbekistan_school_name(df: sql.DataFrame):
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


def standardize_school_name(df: sql.DataFrame):
    # filter
    df1 = df.filter(df.country_code == "UZB")
    df2 = df.filter(df.country_code != "UZB")

    # uzb transform
    df1 = create_uzbekistan_school_name(df1)
    df = df2.union(df1)

    return df


def standardize_internet_speed(df: sql.DataFrame):
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


def rename_raw_columns(df: sql.DataFrame):  ## function for renaming raw files. adhoc
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


def column_mapping_rename(
    df: sql.DataFrame, column_mapping: dict[str | None, str | None]
) -> tuple[sql.DataFrame, dict[str, str]]:
    column_mapping_filtered = {
        k: v for k, v in column_mapping.items() if (k is not None) and (v is not None)
    }
    return df.withColumnsRenamed(column_mapping_filtered), column_mapping_filtered


def add_missing_columns(df: sql.DataFrame, schema_columns: list[StructField]):
    columns_to_add = {
        col.name: f.lit(None).cast(NullType())
        for col in schema_columns
        if col.name not in df.columns
    }
    return df.withColumns(columns_to_add)


def bronze_prereq_columns(df, schema_columns: list[StructField]):
    column_names = [col.name for col in schema_columns]
    df = df.select(*column_names)

    return df


# Note: Temporary function for transforming raw files to standardized columns.
def create_bronze_layer_columns(df: sql.DataFrame, schema_columns: list[StructField]):
    # Impute missing cols with null
    df = add_missing_columns(df, schema_columns)

    # Select required columns for bronze
    df = bronze_prereq_columns(df, schema_columns)

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


def has_critical_error(df: sql.DataFrame):
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


def get_admin1_boundaries(country_code_iso3: str):
    try:
        service = BlobServiceClient(account_url=ACCOUNT_URL, credential=azure_sas_token)
        filename = f"{country_code_iso3}_admin1.geojson"
        file = f"mapbox_sample_data/{filename}"
        blob_client = service.get_blob_client(container=container_name, blob=file)
        with io.BytesIO() as file_blob:
            download_stream = blob_client.download_blob()
            download_stream.readinto(file_blob)
            file_blob.seek(0)
            admin1_boundaries = gpd.read_file(file_blob)

    except:  # noqa: E722
        return None

    return admin1_boundaries


def get_admin1_columns(latitude: float, longitude: float, admin1_boundaries):
    point = get_point(longitude=longitude, latitude=latitude)

    if point is not None and admin1_boundaries is not None:
        for index, row in admin1_boundaries.iterrows():  # noqa: B007
            if row.geometry.contains(point):
                admin1_en = row["name_en"]
                admin1_native = row["name"]
                admin1_id_giga = row["admin1_id_giga"]
                break
            else:
                admin1_en = None
                admin1_native = None
                admin1_id_giga = None
    else:
        admin1_en = None
        admin1_native = None
        admin1_id_giga = None

    return admin1_en, admin1_native, admin1_id_giga


def create_admin1_columns(latitude: float, longitude: float, country_code_iso3: str):
    boundaries = get_admin1_boundaries(country_code_iso3)
    admin1_en, admin1_native, admin1_id_giga = get_admin1_columns(
        latitude, longitude, boundaries
    )
    return print(admin1_en), print(admin1_native), print(admin1_id_giga)


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    #
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BRA_school_geolocation_coverage_master.csv"
    # file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/BLZ_master_reference.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"

    spark = get_spark_session()
    master = spark.read.csv(file_url_master, header=True)
    # reference = spark.read.csv(file_url_reference, header=True)
    # df_bronze = master.join(reference, how="left", on="school_id_giga")

    # df = spark.read.csv(file_url, header=True)
    # df = create_bronze_layer_columns(df)
    # df.show()

    # df = master.select(
    #     [
    #         "school_id_giga",
    #         "school_id_govt",
    #         "school_name",
    #         "education_level",
    #         "latitude",
    #         "longitude",
    #         "admin1",
    #         "admin2"
    #     ]
    # ).limit(20)
    # df = df.withColumn("school_name", f.trim(f.col("school_name")))
    # df = df.withColumn("latitude", f.lit(17.5066649))
    # df = create_school_id_giga(df)
    # df = df.filter(f.col("school_id_giga") == f.col("school_id_giga_test"))
    # df = is_not_within_country(df=df, country_code_iso3="GIN")

    # df = df.withColumn("country_code_iso3", f.lit("BRA"))
    # df = df.withColumn("admin1_test", get_admin1(f.col("latitude"), f.col("longitude"), f.col("country_code_iso3")))
    # df.show()

    # admin1_boundaries = get_admin1_boundaries(country_code_iso3="BRA")
    # latitude = -8.758459
    # longitude = -63.85401
    # print(get_admin1_columns(latitude=latitude, longitude=longitude, admin1_boundaries=admin1_boundaries))

    # print(get_admin1_columns(-8.758459, -63.85401, get_admin1_boundaries("BRA")))
    create_admin1_columns(-8.758459, -63.85401, "BRA")
    create_admin1_columns(8.758459, -63.85401, "BRA")
    create_admin1_columns(8.758459, -63.85401, "BRI")

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
