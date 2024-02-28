from decimal import Decimal
from difflib import SequenceMatcher

import pandas as pd
from h3 import geo_to_h3
from pyspark import Broadcast
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import IntegerType

from .config_expectations import SIMILARITY_RATIO_CUTOFF
from .udf_dependencies import (
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


def has_similar_name_udf_factory(name_list_broadcasted: Broadcast[list]):
    @pandas_udf(IntegerType())
    def has_similar_name(school_name: pd.Series) -> pd.Series:
        result = pd.Series([0] * school_name.size)

        for i, sname in enumerate(school_name):
            for name in name_list_broadcasted.value:
                if SequenceMatcher(None, sname, name).ratio() > SIMILARITY_RATIO_CUTOFF:
                    result.at[i] = 1
                    break

        return result

    return has_similar_name
