from unittest.mock import MagicMock, patch

from pyspark.sql import types
from src.utils.spark import (
    _get_host_ip,
    compute_row_hash,
    count_nulls_for_column,
    transform_columns,
    transform_qos_bra_types,
    transform_school_types,
    transform_types,
)


def test_get_host_ip():
    with patch("src.utils.spark.subprocess.run") as mock_run:
        mock_run.return_value.stdout.strip.return_value.decode.return_value = "1.2.3.4"
        assert _get_host_ip() == "1.2.3.4"

        mock_run.return_value.stdout.strip.return_value.decode.return_value = (
            "127.0.1.1"
        )
        assert _get_host_ip() == "127.0.0.1"


def test_count_nulls_for_column(spark_session):
    data = [("a",), (None,), ("b",)]
    df = spark_session.createDataFrame(data, ["col1"])
    assert count_nulls_for_column(df, "col1") == 3


def test_transform_columns(spark_session):
    data = [("1", "2.5")]
    df = spark_session.createDataFrame(data, ["int_col", "float_col"])

    df_transformed = transform_columns(df, ["int_col"], types.IntegerType())

    assert df_transformed.schema["int_col"].dataType == types.IntegerType()
    assert df_transformed.collect()[0]["int_col"] == 1
    # Check untouched
    assert df_transformed.schema["float_col"].dataType == types.StringType()


def test_transform_school_types(spark_session):
    # Just verify it attempts to cast known columns without error
    # We provide a subset of columns to verify they get picked up
    data = [("10", "1.0", "name")]
    df = spark_session.createDataFrame(
        data, ["num_students", "latitude", "school_name"]
    )

    df_res = transform_school_types(df)

    assert df_res.schema["num_students"].dataType == types.IntegerType()
    assert df_res.schema["latitude"].dataType == types.DoubleType()


def test_transform_qos_bra_types(spark_session):
    data = [("10", "5.5", "1", "1", "2023-01-01")]
    # Include all expected columns to avoid AnalysisException
    df = spark_session.createDataFrame(
        data,
        ["ip_family", "speed_upload", "school_id_govt", "num_students", "timestamp"],
    )

    # Add other columns expected by the function as nulls
    # The function transforms:
    # Ints: ip_family, school_id_govt, num_students, education_level_govt, latency_connectivity
    # Floats: speed_upload, speed_download, latency
    # Timestamp: timestamp

    # We used a minimal set, so we must fill the rest
    expected_cols = [
        "school_id_govt",
        "num_students",
        "education_level_govt",
        "latency_connectivity",
        "speed_download",
        "latency",
        "roundtrip_time",
        "jitter_upload",
        "jitter_download",
        "rtt_packet_loss_pct",
    ]
    for c in expected_cols:
        if c not in df.columns:
            from pyspark.sql.functions import lit

            df = df.withColumn(c, lit(None).cast("string"))

    df_res = transform_qos_bra_types(df)

    assert df_res.schema["ip_family"].dataType == types.IntegerType()
    assert df_res.schema["speed_upload"].dataType == types.FloatType()
    assert "date" in df_res.columns
    assert "id" in df_res.columns


def test_compute_row_hash(spark_session):
    data = [("a", "b"), ("a", "c")]
    df = spark_session.createDataFrame(data, ["col1", "col2"])

    df_hashed = compute_row_hash(df)

    assert "signature" in df_hashed.columns
    res = df_hashed.collect()
    assert res[0]["signature"] != res[1]["signature"]


@patch("src.utils.spark.get_schema_columns")
def test_transform_types(mock_get_schema, spark_session):
    # Mock schema return
    col_def = MagicMock()
    col_def.name = "col1"
    col_def.dataType = types.IntegerType()
    mock_get_schema.return_value = [col_def]

    data = [("1", "abc")]
    df = spark_session.createDataFrame(data, ["col1", "col2"])

    df_res = transform_types(df, "dummy_schema", context=MagicMock())

    assert df_res.schema["col1"].dataType == types.IntegerType()
    assert (
        df_res.schema["col2"].dataType == types.StringType()
    )  # Untouched if not in schema def
