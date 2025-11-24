import io

import country_converter as coco
import fiona
import geopandas as gpd
from pyspark import sql
from pyspark.sql import functions as f

from azure.storage.blob import BlobServiceClient
from dagster import OpExecutionContext
from src.settings import settings
from src.spark.user_defined_functions import (
    is_not_within_country_boundaries_udf_factory,
    is_not_within_country_check_udf_factory,
)
from src.utils.logger import get_context_with_fallback_logger

azure_sas_token = settings.AZURE_SAS_TOKEN
azure_blob_container_name = settings.AZURE_BLOB_CONTAINER_NAME

DUPLICATE_SCHOOL_DISTANCE_KM = 0.1

ACCOUNT_URL = "https://saunigiga.blob.core.windows.net/"
DIRECTORY_LOCATION_GADM = "raw/geospatial-data/gadm_files/version4.1/"
DIRECTORY_LOCATION_MAPBOX = "admin_data/admin0/"
container_name = azure_blob_container_name


def get_country_geometry(country_code_iso3: str):
    try:
        service = BlobServiceClient(account_url=ACCOUNT_URL, credential=azure_sas_token)
        filename = f"{country_code_iso3.upper()}_admin0.geojson"
        file = f"{DIRECTORY_LOCATION_MAPBOX}{filename}"
        blob_client = service.get_blob_client(container=container_name, blob=file)
        with io.BytesIO() as file_blob:
            download_stream = blob_client.download_blob()
            download_stream.readinto(file_blob)
            file_blob.seek(0)
            # setting OGR_GEOJSON_MAX_OBJ_SIZE to 0 ensures we can read geojson files of any size
            with fiona.Env(OGR_GEOJSON_MAX_OBJ_SIZE="0"):
                gdf_boundaries = gpd.read_file(file_blob)
                gdf_boundaries = gdf_boundaries[
                    gdf_boundaries["worldview"].str.contains("US|all")
                ].reset_index(drop=True)
        country_geometry = gdf_boundaries
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

    # geopy constants
    country_code_iso2 = coco.convert(names=[country_code_iso3], to="ISO2")

    is_not_within_country_boundaries = is_not_within_country_boundaries_udf_factory(
        country_code_iso3,
        geometry,
    )

    is_not_within_country_check = is_not_within_country_check_udf_factory(
        country_code_iso2,
        geometry,
    )

    df = df.withColumn(
        "dq_is_not_within_country",
        is_not_within_country_boundaries(f.col("latitude"), f.col("longitude")),
    )

    df = df.withColumn(
        "dq_is_not_within_country",
        is_not_within_country_check(
            f.col("latitude"), f.col("longitude"), f.col("dq_is_not_within_country")
        ),
    )

    df = df.withColumn(
        "dq_is_not_within_country",
        f.when(
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("longitude")),
            f.lit(None).cast("int"),
        ).otherwise(f.col("dq_is_not_within_country")),
    )
    df = df.withColumn(
        "dq_is_not_within_country", f.col("dq_is_not_within_country").cast("int")
    )

    return df


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BRA_school_geolocation_coverage_master.csv"
    # file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master_updates/PHL/PHL_school_geolocation_coverage_master.csv"
    # file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/BLZ_master_reference.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"

    spark = get_spark_session()
    master = spark.read.csv(file_url_master, header=True)
    # df = master.filter(master["admin1"] == "São Paulo")
    # df = master.filter(master["school_id_govt"] == "11000023")
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
    # boundaries = get_country_geometry("BRA")

    # df = df.withColumn("latitude", f.lit(-8.302844))  # outside boundary <= 150km
    # df = df.withColumn("longitude", f.lit(-1.887341)) # outside boundary <= 150km
    # df = df.withColumn("latitude", f.col("latitude").cast("double"))
    # df = df.withColumn("longitude", f.col("longitude").cast("double"))
    # df.show()

    dq_test = is_not_within_country(df, "BRA")
    # dq_test = dq_test.filter(dq_test["dq_is_not_within_country"] == 1)
    dq_test.show()
