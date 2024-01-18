import decimal

# Name Similarity: Can use fuzzywuzzy or thefuzz instead
import difflib

import country_converter as coco

# Geospatial
import geopandas as gpd
import geopy.distance
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from shapely.geometry import Point
from shapely.ops import nearest_points

DUPLICATE_SCHOOL_DISTANCE = 100
DIRECTORY_LOCATION = (  # To Do: Convert to Azure Data Lake Storage
    "great_expectations/uncommitted/notebooks/data/"
)


# CHECK FUNCTIONS
# For checking if location is within country
# For getting the country GADM geometry
def get_country_geometry(country_code_iso3):
    # To Do: Read file from ADLS storage
    directory_location = "great_expectations/uncommitted/notebooks/data/"
    try:
        gdf_boundaries = gpd.read_file(f"{directory_location}{country_code_iso3}.gpkg")
        country_geometry = gdf_boundaries[gdf_boundaries["GID_0"] == country_code_iso3][
            "geometry"
        ][0]
    except ValueError as e:
        if str(e) == "Must be a coordinate pair or Point":
            country_geometry = None
        else:
            raise

    return country_geometry


def get_point(longitude, latitude):
    try:
        point = Point(longitude, latitude)
    except ValueError as e:
        if str(e) == "Must be a coordinate pair or Point":
            point = None
        else:
            raise

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
    location = geolocator.reverse(coords, language="en")
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


# Availability Tests
def is_available(availability):
    return str(availability).strip().lower() == "yes"


def has_value(value):
    return (value is not None) and (value != "")


def has_same_availability(availability, value):
    return is_available(availability) == has_value(value)


# Decimal places tests
def get_decimal_places(number):
    return -decimal.Decimal(str(number)).as_tuple().exponent


def has_at_least_n_decimal_places(number, places):
    return get_decimal_places(number) >= places


# Coordinates should be a tuple
# coords_1 = (latitude, longitude)
# coords_2 = (latitude, longitude)
# distance_km is a float number that indicates the minimum distane
def are_pair_points_beyond_minimum_distance(
    coords_1, coords_2, distance_km=DUPLICATE_SCHOOL_DISTANCE
):
    return geopy.distance.geodesic(coords_1, coords_2).km > distance_km


def are_all_points_beyond_minimum_distance(
    points, distance_km=DUPLICATE_SCHOOL_DISTANCE
):
    # Initial assumption all are far apart
    check_list = [True for i in range(len(points))]

    # Loop through the first and last point
    for i in range(len(points)):
        # Loop through the point after the current point
        for j in range(i + 1, len(points)):
            # When items are shown to be within the prescribed distance, mark it
            if are_pair_points_beyond_minimum_distance(points[i], points[j]) is False:
                check_list[i] = False
                check_list[j] = False

    return check_list


# Checks if the name is not similar to any name in the list
def has_no_similar_name(name_list, similarity_percentage=0.7):
    already_found = []
    is_unique = []
    for string_value in name_list:
        if string_value in already_found:
            is_unique.append(False)
            continue
        matches = difflib.get_close_matches(
            string_value, name_list, cutoff=similarity_percentage
        )
        if len(matches) > 1:
            already_found.extend(matches)
            is_unique.append(False)
        else:
            is_unique.append(True)

    return is_unique


if __name__ == "__main__":
    point_list = [(12.000, 13.000), (12.000, 13.100), (15.000, 15.000)]
    print(are_all_points_beyond_minimum_distance(point_list))
    print(has_no_similar_name(["mawda", "adsads"]))
    print(has_no_similar_name(["mawda", "mawda / B"]))
    print(has_at_least_n_decimal_places(10.35452, 5))
    print(has_at_least_n_decimal_places(10.3545, 5))
    print("Within country")
    print(
        is_within_boundary_distance(
            latitude=16.14626, longitude=-88.72097, country_code_iso3="BLZ"
        )
    )
    latitude = 17.49952
    longitude = -88.19756
    country_code_iso3 = "BLZ"
    print(is_within_country_gadm(latitude, longitude, country_code_iso3))
    print(is_within_country_geopy(latitude, longitude, country_code_iso3))
    print("distance")
    list_values = [1, 2, 3, 4, 5, 6]
    print(list_values[1:])
    print(range(3))
    print(geopy.distance.geodesic((12.000, 13.0009), (12.000, 13.001)).km)
