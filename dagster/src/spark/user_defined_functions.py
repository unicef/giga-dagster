from decimal import Decimal

from h3 import geo_to_h3
from pyspark.sql.functions import udf

from .udf_dependencies import (
    boundary_distance,
    is_within_boundary_distance,
    is_within_country_gadm,
    is_within_country_geopy,
)


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
def point_110_udf(value):
    if value is None:
        point = None
    else:
        point = int(1000 * float(value)) / 1000

    return point


@udf
def h3_geo_to_h3_udf(latitude: float, longitude: float):
    if latitude is None or longitude is None:
        out = "0"
    else:
        out = geo_to_h3(latitude, longitude, resolution=8)

    return out


def is_not_within_country_check_udf_factory(
    country_code_iso2: str, country_code_iso3: str, geometry
):
    @udf
    def is_not_within_country_check(latitude: float, longitude: float) -> int:
        if latitude is None or longitude is None or country_code_iso3 is None:
            out = 0
        else:
            if is_within_country_gadm(latitude, longitude, geometry):
                return 0
            if is_within_boundary_distance(latitude, longitude, geometry):
                return 0
            if boundary_distance(latitude, longitude, geometry) <= 150:
                if is_within_country_geopy(latitude, longitude, country_code_iso2):
                    return 0
            else:
                return 1
        return out

    return is_not_within_country_check
