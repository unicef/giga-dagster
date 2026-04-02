from unittest.mock import patch

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.spark.coverage_transform_functions import (
    coverage_column_filter,
    coverage_row_filter,
    fb_percent_to_boolean,
    fb_transforms,
    itu_binary_to_boolean,
    itu_transforms,
)


def test_coverage_column_filter(spark_session):
    data = [("id1", "val1", "val2")]
    schema = StructType(
        [
            StructField("school_id_giga", StringType(), True),
            StructField("col1", StringType(), True),
            StructField("col2", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    cols_to_keep = ["school_id_giga", "col1"]
    result_df = coverage_column_filter(df, cols_to_keep)
    assert result_df.columns == cols_to_keep


def test_coverage_row_filter(spark_session):
    data = [("id1",), (None,)]
    schema = StructType([StructField("school_id_giga", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)
    result_df = coverage_row_filter(df)
    assert result_df.count() == 1


def test_fb_percent_to_boolean(spark_session):
    data = [(0, 10, 0)]
    schema = StructType(
        [
            StructField("percent_2G", IntegerType(), True),
            StructField("percent_3G", IntegerType(), True),
            StructField("percent_4G", IntegerType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    result_df = fb_percent_to_boolean(df)
    row = result_df.collect()[0]
    assert row["2G_coverage"] is False
    assert row["3G_coverage"] is True
    assert row["4G_coverage"] is False
    assert "percent_2G" not in result_df.columns


def test_itu_binary_to_boolean(spark_session):
    data = [(0, 1, 0, 1)]
    schema = StructType(
        [
            StructField("2g_mobile_coverage", IntegerType(), True),
            StructField("3g_mobile_coverage", IntegerType(), True),
            StructField("4g_mobile_coverage", IntegerType(), True),
            StructField("5g_mobile_coverage", IntegerType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    result_df = itu_binary_to_boolean(df)
    row = result_df.collect()[0]
    assert row["2G_coverage"] is False
    assert row["3G_coverage"] is True
    assert row["4G_coverage"] is False
    assert row["5G_coverage"] is True


@patch("src.spark.coverage_transform_functions.get_schema_columns")
def test_fb_transforms(mock_get_schema, spark_session):
    mock_get_schema.return_value = [StructField("extra_col", StringType(), True)]

    data = [("id1", 10, 0, 0, True)]
    schema = StructType(
        [
            StructField("school_id_giga", StringType(), True),
            StructField("percent_2G", IntegerType(), True),
            StructField("percent_3G", IntegerType(), True),
            StructField("percent_4G", IntegerType(), True),
            StructField("5G_coverage", BooleanType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)

    with patch("src.spark.coverage_transform_functions.config") as mock_config:
        mock_config.FB_COLUMNS = [
            "school_id_giga",
            "2G_coverage",
            "3G_coverage",
            "4G_coverage",
            "5G_coverage",
        ]
        result_df = fb_transforms(df)
        assert "cellular_coverage_type" in result_df.columns
        assert "extra_col" in result_df.columns


@patch("src.spark.coverage_transform_functions.get_schema_columns")
def test_itu_transforms(mock_get_schema, spark_session):
    mock_get_schema.return_value = [StructField("extra_col", StringType(), True)]

    data = [("id1", 1, 0, 0, 0, 1.0, 1.0, 1.0, 1.0, 1.0)]
    schema = StructType(
        [
            StructField("school_id_giga", StringType(), True),
            StructField("2g_mobile_coverage", IntegerType(), True),
            StructField("3g_mobile_coverage", IntegerType(), True),
            StructField("4g_mobile_coverage", IntegerType(), True),
            StructField("5g_mobile_coverage", IntegerType(), True),
            StructField("fiber_node_dist", DoubleType(), True),
            StructField("5g_cell_site_dist", DoubleType(), True),
            StructField("4g_cell_site_dist", DoubleType(), True),
            StructField("3g_cell_site_dist", DoubleType(), True),
            StructField("2g_cell_site_dist", DoubleType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)

    with patch("src.spark.coverage_transform_functions.config") as mock_config:
        mock_config.ITU_COLUMNS = [
            "school_id_giga",
            "2G_coverage",
            "3G_coverage",
            "4G_coverage",
            "5G_coverage",
            "fiber_node_dist",
            "5g_cell_site_dist",
            "4g_cell_site_dist",
            "3g_cell_site_dist",
            "2g_cell_site_dist",
        ]
        mock_config.ITU_COLUMNS_TO_RENAME = []
        result_df = itu_transforms(df)
        assert "cellular_coverage_type" in result_df.columns
        assert "extra_col" in result_df.columns
