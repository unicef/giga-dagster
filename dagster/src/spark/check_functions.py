import decimal
import difflib
import os
import io

# Geospatial
import country_converter as coco
import geopandas as gpd
from azure.storage.blob import BlobServiceClient
from src.spark.config_expectations import SIMILARITY_RATIO_CUTOFF
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from shapely.geometry import Point
from shapely.ops import nearest_points
import pandas as pd

# Name Similarity
from thefuzz import fuzz

# Spark functions
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, BooleanType, ArrayType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

# Auth
from src.settings import Settings # AZURE_SAS_TOKEN, AZURE_BLOB_CONTAINER_NAME
settings_instance = Settings()
azure_sas_token = settings_instance.AZURE_SAS_TOKEN
azure_blob_container_name = settings_instance.AZURE_BLOB_CONTAINER_NAME

DUPLICATE_SCHOOL_DISTANCE_KM = .1

ACCOUNT_URL = "https://saunigiga.blob.core.windows.net/"

# DIRECTORY_LOCATION = "great_expectations/uncommitted/notebooks/data/"
DIRECTORY_LOCATION = "raw/geospatial-data/gadm_files/version4.1/"
container_name = azure_blob_container_name


# CHECK FUNCTIONS
# For checking if location is within country
# For getting the country GADM geometry
def get_country_geometry(country_code_iso3):
    try:
        service = BlobServiceClient(account_url=ACCOUNT_URL, credential=azure_sas_token)
        filename = "{}.gpkg".format(country_code_iso3)
        file = "{}{}".format(DIRECTORY_LOCATION, filename)
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
            country_geometry is None
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


# Inside based on GADM admin boundaries data
def is_within_country_gadm(latitude, longitude, country_code_iso3):
    geometry = get_country_geometry(country_code_iso3)
    point = get_point(longitude, latitude)

    if point is not None and geometry is not None:
        return point.within(geometry)

    return None


# Inside based on geopy
def is_within_country_geopy(latitude, longitude, country_code_iso3):
    country_code_iso2 = coco.convert(names=[country_code_iso3], to="ISO2")
    geolocator = Nominatim(user_agent="schools_geolocation")
    coords = f"{latitude},{longitude}"
    location = geolocator.reverse(coords)

    if location is None:
        return False
    else:
        geopy_country_code_iso2 = location.raw["address"].get("country_code")

        return geopy_country_code_iso2.lower() == country_code_iso2.lower()


# Inside based on boundary distance
def is_within_boundary_distance(latitude, longitude, country_code_iso3):
    point = get_point(longitude, latitude)
    geometry = get_country_geometry(country_code_iso3)

    if point is not None and geometry is not None:
        p1, p2 = nearest_points(geometry, point)
        point1 = p1.coords[0]
        point2 = (longitude, latitude)
        distance = geodesic(point1, point2).km
        return distance <= 1.5  # km

    return None

is_within_boundary_distance_udf = f.udf(is_within_boundary_distance)

# All the checks to verify if location is within the country
def is_within_country(latitude, longitude, country_code_iso3):
    if latitude is None or longitude is None or country_code_iso3 is None:
        return False
    
    is_valid_gadm = is_within_country_gadm(latitude, longitude, country_code_iso3)
    is_valid_geopy = is_within_country_geopy(latitude, longitude, country_code_iso3)
    is_valid_boundary = is_within_boundary_distance(
        latitude, longitude, country_code_iso3
    )

    # Note: Check if valid lat/long values
    return is_valid_gadm | is_valid_geopy | is_valid_boundary

is_within_country_udf = f.udf(is_within_country)


# Availability Tests
def is_available(availability):
    return str(availability).strip().lower() == "yes"


def has_value(value):
    return (value is not None) and (value != "")


def has_same_availability(availability, value):
    return is_available(availability) == has_value(value)


# Decimal places tests
def get_decimal_places(number):
    if number is None:
        return None
    decimal_places = -decimal.Decimal(str(number)).as_tuple().exponent
    return decimal_places

get_decimal_places_udf = f.udf(get_decimal_places)

def has_at_least_n_decimal_places(number, places):
    return get_decimal_places(number) >= places


# Coordinates should be a tuple
# coords_1 = (latitude, longitude)
# coords_2 = (latitude, longitude)
# distance_km is a float number that indicates the minimum distane
def are_pair_points_beyond_minimum_distance(
    coords_1, coords_2 #, distance_km=DUPLICATE_SCHOOL_DISTANCE
):
    return geodesic(coords_1, coords_2).km <= DUPLICATE_SCHOOL_DISTANCE_KM

are_pair_points_beyond_minimum_distance_udf = f.udf(are_pair_points_beyond_minimum_distance)

# def are_all_points_beyond_minimum_distance(coordinates_list):
    # coordinates_list = [coords for coords in coordinates_list if coords != row_coords]
    # for i in range(len(coordinates_list)):
    #     for j in range(i + 1, len(coordinates_list)):
    #         if are_pair_points_beyond_minimum_distance(coordinates_list[i], coordinates_list[j]) is True:
    #             return True
    # row_coords_t = tuple(row_coords)
    # for i in coordinates_list:
    #     i_tuple = tuple(i)
    #     try:
    #         geodesic(row_coords_t, i_tuple).km
    #     except ValueError:
    #         print(f"Invalid coordinates: {row_coords_t}, {i_tuple}")
    #         continue
    #     if geodesic(row_coords_t, i_tuple).km <= 0.1:
    #         return True
    # return False

# are_all_points_beyond_minimum_distance_udf = f.udf(are_all_points_beyond_minimum_distance)

# def are_all_points_beyond_minimum_distance(
#     points, distance_km=DUPLICATE_SCHOOL_DISTANCE_KM
# ):
#     # Initial assumption all are far apart
#     check_list = [True for i in range(len(points))]

#     # Loop through the first and last point
#     for i in range(len(points)):
#         # Loop through the point after the current point
#         for j in range(i + 1, len(points)):
#             # When items are shown to be within the prescribed distance, mark it
#             if are_pair_points_beyond_minimum_distance(points[i], points[j]) is False:
#                 check_list[i] is False
#                 check_list[j] is False

#     return check_list


# Checks if the name is not similar to any name in the list
# Original using difflib
# def has_no_similar_name(name_list, similarity_percentage=SIMILARITY_RATIO_CUTOFF):
#     already_found = []
#     is_unique = []
#     for string_value in name_list:
#         if string_value in already_found:
#             is_unique.append(False)
#             continue

#         # This always loops through whole list to get close matches
#         matches = difflib.get_close_matches(
#             string_value, name_list, cutoff=SIMILARITY_RATIO_CUTOFF
#         )
#         if len(matches) > 1:
#             already_found.extend(matches)
#             is_unique.append(False)
#         else:
#             is_unique.append(True)

#     return is_unique

def has_similar_name(column, name_list):
        for name in name_list:
            if 1 > difflib.SequenceMatcher(None, column, name).ratio() >= SIMILARITY_RATIO_CUTOFF:
                return True
        return False

has_similar_name_udf = f.udf(has_similar_name)

# # Using thefuzz library
# def has_no_similar_name_fuzz(name_list):
#     is_unique = [True for i in range(len(name_list))]

#     # Loop through the first and last index on the list
#     for i in range(len(name_list)):
#         # Skip those already marked with similar match
#         if is_unique[i]:
#             continue

#         # Loop through item after the current index on the list
#         for j in range(i + 1, len(name_list)):
#             # If there is a similar match, then it is no longer unique
#             if fuzz.ratio(name_list[i], name_list[j]) >= SIMILARITY_RATIO_CUTOFF:
#                 is_unique[i] is False
#                 is_unique[j] is False

#     return is_unique


def is_same_name_level_within_radius(row1, row2):
    school_name1 = row1["school_name"]
    education_level1 = row1["education_level"]
    latitude1 = row1["latitude"]
    longitude1 = row1["longitude"]

    school_name2 = row2["school_name"]
    education_level2 = row2["education_level"]
    latitude2 = row2["latitude"]
    longitude2 = row2["longitude"]

    return (
        school_name1 == school_name2
        and education_level1 == education_level2
        and not are_pair_points_beyond_minimum_distance(
            (longitude1, latitude1), (longitude2, latitude2)
        )
    )


def is_similar_name_level_within_radius(row1, row2):
    school_name1 = row1["school_name"]
    education_level1 = row1["education_level"]
    latitude1 = row1["latitude"]
    longitude1 = row1["longitude"]

    school_name2 = row2["school_name"]
    education_level2 = row2["education_level"]
    latitude2 = row2["latitude"]
    longitude2 = row2["longitude"]

    return (
        fuzz.ratio(str(school_name1), str(school_name2)) >= SIMILARITY_RATIO_CUTOFF
        and education_level1 == education_level2
        and not are_pair_points_beyond_minimum_distance(
            (longitude1, latitude1), (longitude2, latitude2)
        )
    )


def duplicate_check(df, check_function):
    is_duplicate = [False for i in range(len(df))]

    # Loop through the first and last rows
    for i in range(len(df)):
        # skip those already marked as duplicate
        if is_duplicate[i]:
            continue

        # Loop through the row after the current row (i)
        for j in range(i + 1, len(df)):
            row1 = df.iloc[i]
            row2 = df.iloc[j]

            # When items are shown to be within the prescribed distance, mark it
            if check_function(row1, row2):
                is_duplicate[i] = True
                is_duplicate[j] = True

    return is_duplicate


def is_valid_range(value, min, max):
    if type(value) == str:
        return False

    is_numeric_min = isinstance(min, int | float)
    is_numeric_max = isinstance(max, int | float)
    if is_numeric_min and not is_numeric_max:
        return value >= min
    if not is_numeric_min and is_numeric_max:
        return value <= max
    if is_numeric_min and is_numeric_max:
        return value >= min and value <= max

    # There is no value range to check
    return False


if __name__ == "__main__":
    # geo = get_country_geometry("BLZ")
    # print(geo)
    # output = is_valid_range(2, 1, 3)
    # print(output)
    # df = pd.DataFrame({'school_name': ["1", "mark", "mark", "marko"],
    #                    'education_level': [3, 4, 4, 4],
    #                    'latitude': [3, 5, 5, 5], 'longitude': [2, 4, 4, 4]})
    # # print(duplicate_check(df, is_same_name_level_within_radius))
    # # print(duplicate_check(df, is_similar_name_level_within_radius))
    # # point_list = [(12.000, 13.000), (12.000, 13.100), (15.000, 15.000)]
    # # print(are_all_points_beyond_minimum_distance(point_list))
    # # print(has_no_similar_name(["mawda", "adsads"]))
    # # print(has_no_similar_name(["mawda", "mawda / B"]))
    # # print(has_at_least_n_decimal_places(10.35452, 5))
    # # print(has_at_least_n_decimal_places(10.3545, 5))
    # print("Within country")
    # print(
    #     is_within_boundary_distance(
    #         latitude=16.14626, longitude=-88.72097, country_code_iso3="BLZ"
    #     )
    # )
    # latitude = 17.49952
    # longitude = -88.19756
    # country_code_iso3 = "BLZ"
    # print(is_within_country_gadm(latitude, longitude, country_code_iso3))
    # print(is_within_country_geopy(latitude, longitude, country_code_iso3))
    # print("distance")
    # list_values = [1, 2, 3, 4, 5, 6]
    # print(list_values[1:])
    # print(range(3))
    # print(geopy.distance.geodesic((12.000, 13.0009), (12.000, 13.001)).km)
    test_geo = get_country_geometry("UZB")
    print(test_geo)