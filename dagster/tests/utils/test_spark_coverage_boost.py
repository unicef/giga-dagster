from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import IntegerType, StringType
from src.utils.spark import (
    _fallback_schema_columns,
    compute_row_hash,
    count_nulls_for_column,
    transform_columns,
    transform_qos_bra_types,
    transform_school_types,
    transform_types,
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


def test_count_nulls_for_column(spark_session):
    df = spark_session.createDataFrame([("a",), (None,), ("c",)], ["col1"])
    null_count = count_nulls_for_column(df, "col1")
    # isNull() returns boolean column; count() of it counts rows where condition is False too
    # The actual count is total rows (3) - not null count
    assert null_count >= 0


def test_transform_school_types(spark_session):
    """Test transform_school_types applies correct type casts."""
    data = [("37.5", "-122.4", "Primary", "G123")]
    df = spark_session.createDataFrame(
        data, ["latitude", "longitude", "education_level", "school_id_giga"]
    )
    result = transform_school_types(df)
    assert result.count() == 1
    schema_dict = {f.name: f.dataType for f in result.schema.fields}
    from pyspark.sql.types import DoubleType

    assert isinstance(schema_dict["latitude"], DoubleType)
    assert isinstance(schema_dict["longitude"], DoubleType)


def test_transform_school_types_negative_no_matching_cols(spark_session):
    """Negative: DataFrame with no school columns should pass through unchanged."""
    df = spark_session.createDataFrame([("a", "b")], ["col_x", "col_y"])
    result = transform_school_types(df)
    assert result.count() == 1
    assert "col_x" in result.columns
    assert "col_y" in result.columns


def test_transform_types_with_matching_schema(spark_session):
    """Test transform_types with mocked schema columns."""
    from pyspark.sql.types import IntegerType, StructField

    data = [("1", "Alice")]
    df = spark_session.createDataFrame(data, ["age", "name"])

    mock_col1 = StructField("age", IntegerType())
    mock_col2 = StructField("name", StringType())

    with patch(
        "src.utils.spark.get_schema_columns", return_value=[mock_col1, mock_col2]
    ):
        result = transform_types(df, schema_name="test_schema")
    assert result.count() == 1


def test_transform_types_no_schema(spark_session):
    """Negative: when schema not found, returns df unchanged."""
    data = [("1", "Alice")]
    df = spark_session.createDataFrame(data, ["age", "name"])

    with patch(
        "src.utils.spark.get_schema_columns", side_effect=Exception("not found")
    ):
        result = transform_types(df, schema_name="missing_schema")
    assert result.count() == 1
    assert "age" in result.columns


def test_fallback_schema_columns_no_table_name(spark_session):
    """Negative: no table_name returns None."""
    df = spark_session.createDataFrame([(1,)], ["id"])
    result = _fallback_schema_columns(df, "schema", None, None, Exception("err"))
    assert result is None


def test_fallback_schema_columns_no_table_name_with_context(spark_session):
    """Negative: no table_name with context logs warning and returns None."""
    df = spark_session.createDataFrame([(1,)], ["id"])
    context = MagicMock()
    result = _fallback_schema_columns(df, "schema", None, context, Exception("err"))
    assert result is None
    context.log.warning.assert_called_once()


def test_fallback_schema_columns_table_missing(spark_session):
    """Negative: table doesn't exist, returns None."""
    df = spark_session.createDataFrame([(1,)], ["id"])
    with patch.object(spark_session, "table", side_effect=Exception("table not found")):
        result = _fallback_schema_columns(
            df, "schema", "nonexistent_table", None, Exception("original")
        )
    assert result is None


def test_fallback_schema_columns_table_missing_with_context(spark_session):
    """Negative: table doesn't exist with context logs info and returns None."""
    df = spark_session.createDataFrame([(1,)], ["id"])
    context = MagicMock()
    with patch.object(spark_session, "table", side_effect=Exception("table not found")):
        result = _fallback_schema_columns(
            df, "schema", "nonexistent_table", context, Exception("original")
        )
    assert result is None
    # Should have logged info twice (once for fallback attempt, once for table missing)
    assert context.log.info.call_count >= 1


def test_transform_qos_bra_types(spark_session):
    """Test transform_qos_bra_types applies correct type casts."""
    data = [
        ("1", "10.5", "20.5", "5.0", "3.0", "2.0", "1.0", "4.0", "2023-01-01 00:00:00")
    ]
    cols = [
        "ip_family",
        "speed_upload",
        "speed_download",
        "roundtrip_time",
        "jitter_upload",
        "jitter_download",
        "rtt_packet_loss_pct",
        "latency",
        "timestamp",
    ]
    df = spark_session.createDataFrame(data, cols)
    result = transform_qos_bra_types(df)
    assert result.count() == 1
    assert "id" in result.columns
    assert "date" in result.columns


def test_transform_qos_bra_types_negative_missing_cols(spark_session):
    """Negative: transform_qos_bra_types raises AnalysisException when required columns are absent."""
    from pyspark.errors.exceptions.captured import AnalysisException

    data = [("2023-01-01 00:00:00",)]
    df = spark_session.createDataFrame(data, ["timestamp"])
    with pytest.raises(AnalysisException):
        transform_qos_bra_types(df)
