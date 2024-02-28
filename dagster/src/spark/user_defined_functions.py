from decimal import Decimal

from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from h3 import geo_to_h3
from pyspark.sql.functions import udf
from shapely.geometry import Point
from shapely.ops import nearest_points

from src.settings import settings


def get_decimal_places_udf_factory(precision: int):
    @udf
    def get_decimal_places(value):
        if value is None:
            out = None
        else:
            decimal_places = -Decimal(str(value)).as_tuple().exponent
            out = int(decimal_places < precision)
        return out

    return get_decimal_places


@udf
def point_110(value):
    if value is None:
        point = None
    else:
        point = int(1000 * float(value)) / 1000

    return point


@udf
def h3_geo_to_h3(latitude: float, longitude: float):
    if latitude is None or longitude is None:
        out = "0"
    else:
        out = geo_to_h3(latitude, longitude, resolution=8)

    return out


def get_point(longitude: float, latitude: float):
    try:
        point = Point(longitude, latitude)
    except:  # noqa: E722
        point = None

    return point


# Inside based on GADM admin boundaries data
def is_within_country_gadm(latitude: float, longitude: float, geometry) -> bool:
    point = get_point(longitude, latitude)
    if point is not None and geometry is not None:
        out = point.within(geometry)
    else:
        out = False
    return out


# Inside based on geopy
def is_within_country_geopy(
    latitude: float, longitude: float, country_code_iso2: str
) -> bool:
    geolocator = Nominatim(user_agent=f"GigaSyncDagster/{settings.COMMIT_SHA[:7]}")
    coords = f"{latitude},{longitude}"

    if latitude is None or longitude is None:
        out = False
    else:
        location = geolocator.reverse(coords, timeout=10)
        geopy_country_code_iso2 = location.raw.get("address", {}).get(
            "country_code", ""
        )
        out = geopy_country_code_iso2.lower() == country_code_iso2.lower()

    return out


# Inside based on boundary distance
def is_within_boundary_distance(latitude: float, longitude: float, geometry) -> bool:
    point = get_point(longitude, latitude)

    if point is not None and geometry is not None:
        p1, p2 = nearest_points(geometry, point)
        point1 = p1.coords[0]
        point2 = (longitude, latitude)
        distance = geodesic(point1, point2).km
        out = distance <= 1.5  # km
    else:
        out = False

    return out


def is_not_within_country_check_udf_factory(
    country_code_iso2: str, country_code_iso3: str, geometry
):
    @udf
    def is_not_within_country_check(latitude: float, longitude: float) -> int:
        if latitude is None or longitude is None or country_code_iso3 is None:
            out = 0
        else:
            is_valid_gadm = is_within_country_gadm(latitude, longitude, geometry)
            is_valid_geopy = is_within_country_geopy(
                latitude, longitude, country_code_iso2
            )
            is_valid_boundary = is_within_boundary_distance(
                latitude, longitude, geometry
            )

            is_valid = any([is_valid_gadm, is_valid_geopy, is_valid_boundary])
            out = int(not is_valid)

        return out

    return is_not_within_country_check
