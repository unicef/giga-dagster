from unittest.mock import patch

from src.spark.coverage_transform_functions import (
    coverage_column_filter,
    coverage_row_filter,
    fb_percent_to_boolean,
    fb_transforms,
    itu_binary_to_boolean,
    itu_lower_columns,
)


def test_coverage_column_filter(spark_session):
    df = spark_session.createDataFrame([("a", "b")], ["col1", "col2"])
    res = coverage_column_filter(df, ["col1"])
    assert res.columns == ["col1"]


def test_coverage_row_filter(spark_session):
    data = [("1",), (None,)]
    df = spark_session.createDataFrame(data, ["school_id_giga"])
    res = coverage_row_filter(df)
    assert res.count() == 1


def test_fb_percent_to_boolean(spark_session):
    data = [(10, 0, 0)]
    df = spark_session.createDataFrame(data, ["percent_2G", "percent_3G", "percent_4G"])
    res = fb_percent_to_boolean(df)
    assert "2G_coverage" in res.columns
    assert res.select("2G_coverage").collect()[0][0] is True
    assert res.select("3G_coverage").collect()[0][0] is False


def test_itu_binary_to_boolean(spark_session):
    data = [(1, 0, 0, 1)]
    df = spark_session.createDataFrame(
        data,
        [
            "2g_mobile_coverage",
            "3g_mobile_coverage",
            "4g_mobile_coverage",
            "5g_mobile_coverage",
        ],
    )
    res = itu_binary_to_boolean(df)
    assert res.select("2G_coverage").collect()[0][0] is True
    assert res.select("5G_coverage").collect()[0][0] is True


def test_itu_lower_columns(spark_session):
    with patch("src.spark.coverage_transform_functions.config") as mock_conf:
        mock_conf.ITU_COLUMNS_TO_RENAME = ["Col1"]
        df = spark_session.createDataFrame([(1,)], ["Col1"])
        res = itu_lower_columns(df)
        assert "col1" in res.columns
        assert "Col1" not in res.columns


def test_fb_transforms(spark_session):
    from pyspark.sql.types import StringType, StructField

    with (
        patch("src.spark.coverage_transform_functions.config") as mock_conf,
        patch(
            "src.spark.coverage_transform_functions.get_schema_columns"
        ) as mock_schema,
    ):
        mock_conf.FB_COLUMNS = [
            "school_id_giga",
            "2G_coverage",
            "3G_coverage",
            "4G_coverage",
            "5G_coverage",
        ]

        mock_schema.return_value = [StructField("new_col", StringType(), True)]

        data = [("1", 10, 0, 0, False)]
        df = spark_session.createDataFrame(
            data,
            ["school_id_giga", "percent_2G", "percent_3G", "percent_4G", "5G_coverage"],
        )

        res = fb_transforms(df)

        assert "cellular_coverage_type" in res.columns
        assert "cellular_coverage_availability" in res.columns
        assert "new_col" in res.columns

        row = res.collect()[0]
        assert row.cellular_coverage_type == "2G"
