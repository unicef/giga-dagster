from decimal import Decimal
from difflib import SequenceMatcher
from math import isnan

import numpy as np
import pandas as pd
from h3 import latlng_to_cell as geo_to_h3
from pyspark.sql.functions import pandas_udf, udf

from src.spark.config_expectations import config

from .udf_dependencies import (
    is_within_boundary_distance,
    is_within_country_mapbox,
)


def get_decimal_places_udf_factory(precision: int) -> callable:
    @udf
    def get_decimal_places(value) -> int | None:
        if value is None:
            return None
        try:
            decimal_places = -Decimal(str(value)).as_tuple().exponent
        except TypeError:
            return None
        return int(decimal_places < precision)

    return get_decimal_places


@udf
def point_110_udf(value) -> float | None:
    if value is None:
        return None
    try:
        x = float(value)
        if isnan(x):
            return None
    except ValueError:
        return None

    return int(1000 * float(value)) / 1000


@udf
def h3_geo_to_h3_udf(latitude: float, longitude: float) -> str:
    if latitude is None or longitude is None:
        return "0"

    return geo_to_h3(latitude, longitude, 8)


BOUNDARY_DISTANCE_THRESHOLD = 150


def is_not_within_country_boundaries_udf_factory(
    country_code_iso3: str,
    geometry: pd.Series | np.ndarray | pd.DataFrame,
) -> callable:
    @pandas_udf("int")
    def is_not_within_country_check(
        latitude: pd.Series, longitude: pd.Series
    ) -> pd.Series:
        return is_within_country_mapbox(latitude, longitude, geometry)

    return is_not_within_country_check


def is_not_within_country_check_udf_factory(
    country_code_iso2: str,
    geometry: pd.Series | np.ndarray | pd.DataFrame,
) -> callable:
    geometry = geometry["geometry"][0]

    @udf
    def is_not_within_country_check(
        latitude: float, longitude: float, dq_is_not_within_country: int
    ) -> int:
        if is_within_boundary_distance(
            latitude, longitude, geometry, dq_is_not_within_country
        ):
            return 0

        return 1

    return is_not_within_country_check


@udf
def has_similar_name_check_udf(school_name, school_name_2) -> int:
    if school_name is None or school_name_2 is None:
        return 0

    if (
        school_name != school_name_2
        and SequenceMatcher(a=school_name, b=school_name_2).ratio()
        > config.SIMILARITY_RATIO_CUTOFF
    ):
        return 1

    return 0
