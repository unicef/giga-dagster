from unittest.mock import patch

from pyspark.sql import (
    Row,
    functions as f,
)
from src.data_quality_checks.geography import is_not_within_country


def mock_bound_impl(lat, lon):
    return f.when(lat == 20.0, f.lit(1)).otherwise(f.lit(0))


def mock_check_impl(lat, lon, boundary_res):
    return boundary_res


def test_is_not_within_country(spark_session):
    with (
        patch(
            "src.data_quality_checks.geography.get_country_geometry"
        ) as mock_get_geom,
        patch("src.data_quality_checks.geography.coco.convert") as mock_convert,
        patch(
            "src.data_quality_checks.geography.is_not_within_country_boundaries_udf_factory"
        ) as mock_udf_factory_bound,
        patch(
            "src.data_quality_checks.geography.is_not_within_country_check_udf_factory"
        ) as mock_udf_factory_check,
    ):
        mock_get_geom.return_value = "geometry_obj"
        mock_convert.return_value = "BR"
        mock_udf_factory_bound.return_value = mock_bound_impl
        mock_udf_factory_check.return_value = mock_check_impl
        data = [
            Row(latitude=10.0, longitude=10.0),
            Row(latitude=20.0, longitude=10.0),
            Row(latitude=None, longitude=10.0),
        ]
        df = spark_session.createDataFrame(data)
        res = is_not_within_country(df, "BRA")
        rows = res.sort(f.col("latitude").asc_nulls_last()).collect()
        assert rows[0]["dq_is_not_within_country"] == 0
        assert rows[1]["dq_is_not_within_country"] == 1
        assert rows[2]["dq_is_not_within_country"] is None
