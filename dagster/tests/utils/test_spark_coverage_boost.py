from pyspark.sql.types import IntegerType, StringType
from src.utils.spark import (
    compute_row_hash,
    transform_columns,
)


def test_compute_row_hash(spark_session):
    df = spark_session.createDataFrame(
        [
            {"id": 1, "name": "Alice", "city": "NYC"},
            {"id": 2, "name": "Bob", "city": "LA"},
        ]
    )

    result = compute_row_hash(df)

    assert "signature" in result.columns
    assert result.count() == 2
    signatures = result.select("signature").collect()
    assert all(len(row[0]) == 64 for row in signatures)


def test_compute_row_hash_idempotent(spark_session):
    df = spark_session.createDataFrame([{"id": 1, "name": "Test"}])

    result1 = compute_row_hash(df)
    result2 = compute_row_hash(df)

    sig1 = result1.select("signature").first()[0]
    sig2 = result2.select("signature").first()[0]
    assert sig1 == sig2


def test_transform_columns_to_string(spark_session):
    df = spark_session.createDataFrame(
        [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
    )

    result = transform_columns(df, ["id", "value"], StringType())

    schema_dict = {field.name: field.dataType for field in result.schema.fields}
    assert isinstance(schema_dict["id"], StringType)
    assert isinstance(schema_dict["value"], StringType)


def test_transform_columns_to_integer(spark_session):
    df = spark_session.createDataFrame(
        [{"id": "1", "value": "100"}, {"id": "2", "value": "200"}]
    )

    result = transform_columns(df, ["id", "value"], IntegerType())

    schema_dict = {field.name: field.dataType for field in result.schema.fields}
    assert isinstance(schema_dict["id"], IntegerType)
    assert isinstance(schema_dict["value"], IntegerType)


def test_transform_columns_missing_column(spark_session):
    df = spark_session.createDataFrame([{"id": 1}])

    result = transform_columns(df, ["id", "nonexistent"], StringType())

    assert result.count() == 1
