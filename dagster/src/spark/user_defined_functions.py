from decimal import Decimal
from difflib import SequenceMatcher

from h3 import geo_to_h3
from pyspark.sql.functions import udf

from src.spark.config_expectations import config

from .udf_dependencies import (
    boundary_distance,
    is_within_boundary_distance,
    is_within_country_gadm,
    is_within_country_geopy,
)


def get_decimal_places_udf_factory(precision: int) -> callable:
    @udf
    def get_decimal_places(value) -> int:
        if value is None:
            return None

        decimal_places = -Decimal(str(value)).as_tuple().exponent
        return int(decimal_places < precision)

    return get_decimal_places


@udf
def point_110_udf(value) -> float | None:
    if value is None:
        return None

    return int(1000 * float(value)) / 1000


@udf
def h3_geo_to_h3_udf(latitude: float, longitude: float) -> str:
    if latitude is None or longitude is None:
        return "0"

    return geo_to_h3(latitude, longitude, resolution=8)


BOUNDARY_DISTANCE_THRESHOLD = 150


def is_not_within_country_check_udf_factory(
    country_code_iso2: str,
    country_code_iso3: str,
    geometry,
) -> callable:
    @udf
    def is_not_within_country_check(latitude: float, longitude: float) -> int:
        if latitude is None or longitude is None or country_code_iso3 is None:
            return 0

        if is_within_country_gadm(latitude, longitude, geometry):
            return 0
        if is_within_boundary_distance(latitude, longitude, geometry):
            return 0
        if (
            boundary_distance(latitude, longitude, geometry)
            <= BOUNDARY_DISTANCE_THRESHOLD
        ) and is_within_country_geopy(latitude, longitude, country_code_iso2):
            return 0

        return 1

    return is_not_within_country_check


@udf
def has_similar_name_check_udf(school_name, school_name_2) -> int:
    if (
        school_name != school_name_2
        and SequenceMatcher(None, school_name, school_name_2).ratio()
        > config.SIMILARITY_RATIO_CUTOFF
    ):
        return 1
    return 0
