from unittest.mock import patch

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from src.spark.transform_functions import (
    create_bronze_layer_columns,
    create_school_id_giga,
    standardize_internet_speed,
    standardize_school_name,
)


def test_create_school_id_giga(spark_session):
    data = [
        ("UUID1", "123", "SCH1", "GOVT1", "Primary", "10", "10"),
        ("UUID2", None, "SCH2", "GOVT2", "Secondary", "20", "20"),
    ]
    schema = StructType(
        [
            StructField("uuid", StringType(), True),
            StructField("school_id_giga", StringType(), True),
            StructField("school_name", StringType(), True),
            StructField("school_id_govt", StringType(), True),
            StructField("education_level", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)

    res = create_school_id_giga(df)
    rows = res.collect()
    rows.sort(key=lambda x: x["school_name"])

    assert rows[0]["school_id_giga"] == "123"

    assert rows[1]["school_id_giga"] is not None
    assert rows[1]["school_id_giga"] != "UUID2"


@patch("src.spark.transform_functions.create_uzbekistan_school_name")
def test_standardize_school_name(mock_uzb, spark_session):
    def side_effect(df):
        return df.withColumn("school_name", F.lit("UZB_Processed"))

    mock_uzb.side_effect = side_effect

    data = [("BRA", "School A"), ("UZB", "School B")]
    schema = StructType(
        [
            StructField("country_code", StringType(), True),
            StructField("school_name", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)

    res = standardize_school_name(df)
    rows = res.collect()

    row_bra = next(r for r in rows if r["country_code"] == "BRA")
    row_uzb = next(r for r in rows if r["country_code"] == "UZB")

    assert row_bra["school_name"] == "School A"
    assert row_uzb["school_name"] == "UZB_Processed"


def test_standardize_internet_speed(spark_session):
    data = [("100 Mbps",), ("50",), ("abc",)]
    schema = StructType([StructField("download_speed_govt", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)

    res = standardize_internet_speed(df)
    rows = res.collect()

    assert rows[0]["download_speed_govt"] == 100.0
    assert rows[1]["download_speed_govt"] == 50.0
    assert rows[2]["download_speed_govt"] is None


def test_create_bronze_layer_columns(spark_session):
    data = [("val1",)]
    schema = StructType([StructField("school_id_giga", StringType(), True)])
    df = spark_session.createDataFrame(data, schema)

    silver_data = [("id1", "val1", "ver1", "GOVT1")]

    silver_schema = StructType(
        [
            StructField("school_id_giga", StringType(), True),
            StructField("col1", StringType(), True),
            StructField("version", StringType(), True),
            StructField("school_id_govt", StringType(), True),
        ]
    )
    silver = spark_session.createDataFrame(silver_data, silver_schema)

    data = [("val1", "GOVT1")]
    schema = StructType(
        [
            StructField("col1_input", StringType(), True),
            StructField("school_id_govt", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)

    res = create_bronze_layer_columns(df, silver, "BRA", "create", ["col1"])

    assert "school_id_giga" in res.columns
    assert "version" in res.columns
    assert "school_id_govt" in res.columns
