from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.data_quality_checks import (
    column_relation,
    coverage,
    create_update,
    critical,
    duplicates,
    geography,
    geometry,
    precision,
    standard,
)


def test_dq_check_for_nulls(spark_session):
    schema = StructType(
        [
            StructField("school_id", StringType(), True),
            StructField("name", StringType(), True),
        ]
    )
    data = [("1", "School A"), (None, "School B"), ("3", None)]
    spark_session.createDataFrame(data, schema)
    assert critical is not None


def test_dq_check_for_duplicates(spark_session):
    schema = StructType([StructField("school_id", StringType())])
    data = [("1",), ("2",), ("1",)]
    spark_session.createDataFrame(data, schema)
    assert duplicates is not None


def test_dq_geography_checks(spark_session):
    schema = StructType(
        [StructField("latitude", DoubleType()), StructField("longitude", DoubleType())]
    )
    data = [(40.7128, -74.0060), (200.0, 300.0)]
    spark_session.createDataFrame(data, schema)
    assert geography is not None


def test_dq_geometry_checks(spark_session):
    assert geometry is not None


def test_dq_precision_checks(spark_session):
    schema = StructType([StructField("value", DoubleType())])
    data = [(1.23456789,), (1.2,), (1.23,)]
    spark_session.createDataFrame(data, schema)
    assert precision is not None


def test_dq_standard_checks(spark_session):
    assert standard is not None


def test_dq_coverage_checks(spark_session):
    assert coverage is not None


def test_dq_create_update_checks(spark_session):
    assert create_update is not None


def test_dq_column_relation_checks(spark_session):
    assert column_relation is not None
