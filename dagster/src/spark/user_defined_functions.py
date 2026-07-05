from decimal import Decimal, InvalidOperation
from math import isnan

import pandas as pd
from h3 import geo_to_h3
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import ArrayType, StringType
from rapidfuzz.fuzz import ratio as fuzzy_string_similarity_ratio

from src.spark.config_expectations import config

from .udf_dependencies import (
    is_within_boundary_distance,
    is_within_country_mapbox,
)


def get_decimal_places_udf_factory(precision: int) -> callable:
    @pandas_udf("int")
    def check_decimal_precision(column_values: pd.Series) -> pd.Series:
        def compute_precision_flag(value):
            if value is None or pd.isna(value):
                return pd.NA
            try:
                decimal_places = -Decimal(str(value)).as_tuple().exponent
                return int(decimal_places < precision)
            except (TypeError, InvalidOperation):
                return pd.NA

        return column_values.apply(compute_precision_flag).astype(pd.Int32Dtype())

    return check_decimal_precision


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


@pandas_udf(StringType())
def h3_geo_to_h3_udf(latitude: pd.Series, longitude: pd.Series) -> pd.Series:
    valid_coordinate_mask = latitude.notna() & longitude.notna()
    h3_cell_index = pd.Series("0", index=latitude.index, dtype=str)
    h3_cell_index[valid_coordinate_mask] = [
        geo_to_h3(lat, lon, resolution=8)
        for lat, lon in zip(
            latitude[valid_coordinate_mask],
            longitude[valid_coordinate_mask],
            strict=False,
        )
    ]
    return h3_cell_index


def is_not_within_country_boundaries_udf_factory(
    country_code_iso3: str,
    boundary_geometry,
) -> callable:
    @pandas_udf("int")
    def spatial_boundary_containment_check(
        latitude: pd.Series, longitude: pd.Series
    ) -> pd.Series:
        boundary_geodataframe = (
            boundary_geometry.value
            if hasattr(boundary_geometry, "value")
            else boundary_geometry
        )
        return is_within_country_mapbox(latitude, longitude, boundary_geodataframe)

    return spatial_boundary_containment_check


def is_not_within_country_check_udf_factory(
    country_code_iso2: str,
    boundary_geometry,
) -> callable:
    boundary_geodataframe = (
        boundary_geometry.value
        if hasattr(boundary_geometry, "value")
        else boundary_geometry
    )
    country_boundary_polygon = boundary_geodataframe["geometry"][0]

    @pandas_udf("int")
    def spatial_boundary_distance_check(
        latitude: pd.Series,
        longitude: pd.Series,
        dq_is_not_within_country: pd.Series,
    ) -> pd.Series:
        return pd.Series(
            [
                0
                if is_within_boundary_distance(
                    lat, lon, country_boundary_polygon, dq_flag
                )
                else 1
                for lat, lon, dq_flag in zip(
                    latitude, longitude, dq_is_not_within_country, strict=False
                )
            ]
        )

    return spatial_boundary_distance_check


@pandas_udf(ArrayType(StringType()))
def find_similar_names_in_group_udf(grouped_school_names: pd.Series) -> pd.Series:
    def find_similar_names_within_group(school_name_list):
        if school_name_list is None or len(school_name_list) == 0:
            return []

        similar_names = set()
        for i, name_a in enumerate(school_name_list):
            if name_a is None:
                continue
            for name_b in school_name_list[i + 1 :]:
                if name_b is None:
                    continue
                if (
                    name_a != name_b
                    and fuzzy_string_similarity_ratio(name_a, name_b) / 100
                    > config.SIMILARITY_RATIO_CUTOFF
                ):
                    similar_names.add(name_a)
                    similar_names.add(name_b)

        return list(similar_names)

    return grouped_school_names.apply(find_similar_names_within_group)
