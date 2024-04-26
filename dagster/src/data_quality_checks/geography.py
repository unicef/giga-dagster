import io

import country_converter as coco
import geopandas as gpd
from pyspark import sql
from pyspark.sql import functions as f

from azure.storage.blob import BlobServiceClient
from dagster import OpExecutionContext
from src.settings import settings
from src.spark.user_defined_functions import (
    is_not_within_country_check_udf_factory,
)
from src.utils.logger import get_context_with_fallback_logger

azure_sas_token = settings.AZURE_SAS_TOKEN
azure_blob_container_name = settings.AZURE_BLOB_CONTAINER_NAME

DUPLICATE_SCHOOL_DISTANCE_KM = 0.1

ACCOUNT_URL = "https://saunigiga.blob.core.windows.net/"
DIRECTORY_LOCATION = "raw/geospatial-data/gadm_files/version4.1/"
container_name = azure_blob_container_name


def get_country_geometry(country_code_iso3: str):
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
            return None
        else:
            raise e

    return country_geometry


def is_not_within_country(
    df: sql.DataFrame,
    country_code_iso3: str,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Checking if not within country...")

    # boundary constants
    geometry = get_country_geometry(country_code_iso3)

    # # broadcast to workers -- > not working yet
    # spark = df.sparkSession
    # broadcasted_geometry = spark.sparkContext.broadcast(geometry)

    # geopy constants
    country_code_iso2 = coco.convert(names=[country_code_iso3], to="ISO2")

    is_not_within_country_check = is_not_within_country_check_udf_factory(
        country_code_iso2,
        country_code_iso3,
        geometry,
    )

    return df.withColumn(
        "dq_is_not_within_country",
        is_not_within_country_check(f.col("latitude"), f.col("longitude")),
    )


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    #
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BRA_school_geolocation_coverage_master.csv"
    file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master_updates/PHL/PHL_school_geolocation_coverage_master.csv"
    # file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/BLZ_master_reference.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"

    spark = get_spark_session()
    master = spark.read.csv(file_url_master, header=True)
    # df = master.filter(master["admin1"] == "São Paulo")
    # df = master.limit(1)
    df = master.select(
        [
            "school_id_giga",
            "school_id_govt",
            "school_name",
            "education_level",
            "latitude",
            "longitude",
        ],
    )
    df.show()
    # df = df.withColumn("latitude", f.lit(6.1671))  # outside boundary <= 150km
    # df = df.withColumn("longitude", f.lit(60.7832)) # outside boundary <= 150km
    # df = df.withColumn("latitude", f.col("latitude").cast("double"))
    # df = df.withColumn("longitude", f.col("longitude").cast("double"))

    dq_test = is_not_within_country(df, "PHL")
    dq_test.show()
    print(dq_test.count())
