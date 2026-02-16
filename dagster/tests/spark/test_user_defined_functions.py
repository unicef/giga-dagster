from unittest.mock import MagicMock, patch

import pandas as pd
from pyspark.sql import functions as F
from src.spark.user_defined_functions import (
    find_similar_names_in_group_udf,
    get_decimal_places_udf_factory,
    h3_geo_to_h3_udf,
    has_similar_name_check_udf,
    is_not_within_country_boundaries_udf_factory,
    point_110_udf,
)


def test_point_110_udf(spark_session):
    data = [(10.1234, 10.123), (10.1, 10.1), (None, None), (float("nan"), None)]
    df = spark_session.createDataFrame(data, ["input", "expected"])

    res = df.withColumn("actual", point_110_udf(F.col("input")))
    rows = res.collect()

    for row in rows:
        if row.expected is None:
            assert row.actual is None
        else:
            assert float(row.actual) == float(row.expected)


def test_get_decimal_places(spark_session):
    udf_2_places = get_decimal_places_udf_factory(2)
    data = [(0.1, 1), (0.01, 0), (None, None)]
    df = spark_session.createDataFrame(data, ["input", "expected"])
    res = df.withColumn("actual", udf_2_places(F.col("input")))
    rows = res.collect()
    for row in rows:
        if row.expected is None:
            assert row.actual is None
        else:
            assert int(row.actual) == int(row.expected)


def test_has_similar_name_check_udf(spark_session):
    with patch("src.spark.user_defined_functions.config") as mock_config:
        mock_config.SIMILARITY_RATIO_CUTOFF = 0.9
        data = [
            ("School A", "School A", 0),
            ("School A", "Different", 0),
            (None, "A", 0),
        ]
        df = spark_session.createDataFrame(data, ["n1", "n2", "exp"])
        res = df.withColumn("act", has_similar_name_check_udf(F.col("n1"), F.col("n2")))
        row = res.collect()[0]
        assert int(row.act) == int(row.exp)


def test_h3_geo_to_h3_udf_logic():
    udf_func = h3_geo_to_h3_udf.func

    with patch("src.spark.user_defined_functions.geo_to_h3") as mock_h3:
        mock_h3.return_value = "h3_index"

        lat = pd.Series([10.0, None])
        lon = pd.Series([20.0, None])

        res = udf_func(lat, lon)

        assert res[0] == "h3_index"
        assert res[1] == "0"


def test_find_similar_names_in_group_logic():
    with (
        patch("src.spark.user_defined_functions.config") as mock_config,
        patch(
            "src.spark.user_defined_functions.has_similar_name_check_udf"
        ) as mock_check,
    ):
        mock_config.SIMILARITY_RATIO_CUTOFF = 0.8
        mock_check.side_effect = lambda a, b: 1 if a[0] == b[0] else 0

        data = pd.Series([["School A", "School A."], ["A", "B"]])

        udf_func = find_similar_names_in_group_udf.func

        res = udf_func(data)

        sim0 = res[0]
        assert len(sim0) == 2


def test_is_not_within_country(spark_session):
    with patch(
        "src.spark.user_defined_functions.is_within_country_mapbox"
    ) as mock_mapbox:
        mock_mapbox.return_value = pd.Series([1, 0])

        factory = is_not_within_country_boundaries_udf_factory("TST", MagicMock())

        data = [(10.0, 20.0), (30.0, 40.0)]
        df = spark_session.createDataFrame(data, ["lat", "lon"])
        df.withColumn("check", factory(F.col("lat"), F.col("lon")))

        udf_func = factory.func

        lat_series = pd.Series([10.0, 30.0])
        lon_series = pd.Series([20.0, 40.0])

        res_series = udf_func(lat_series, lon_series)

        assert res_series.tolist() == [1, 0]
        mock_mapbox.assert_called()
