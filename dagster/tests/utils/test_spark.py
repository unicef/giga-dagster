from unittest.mock import MagicMock, patch

from pyspark.sql.types import DoubleType, LongType, StringType, StructField
from src.utils.spark import transform_types


def test_transform_types_fallback_to_delta_table():
    """When metaschema is missing but target Delta table exists,
    transform_types should fall back to the Delta table schema."""
    mock_df = MagicMock()
    mock_df.columns = ["name", "age", "score"]
    mock_df.schema.simpleString.return_value = "struct<name:string,age:int,score:int>"

    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark

    mock_target_table = MagicMock()
    mock_target_table.schema.fields = [
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("score", DoubleType(), True),
    ]
    mock_spark.table.return_value = mock_target_table

    with (
        patch(
            "src.utils.spark.get_schema_columns",
            side_effect=Exception("Table not found"),
        ),
        patch("src.utils.spark.col", MagicMock()),
    ):
        transform_types(
            mock_df,
            schema_name="giga_meter",
            table_name="connectivity_ping_checks",
        )

    # Should have queried the actual Delta table as fallback
    mock_spark.table.assert_called_once_with("giga_meter.connectivity_ping_checks")

    # Should have called withColumns to cast types
    assert mock_df.withColumns.called

    call_args = mock_df.withColumns.call_args[0][0]
    assert "name" in call_args
    assert "age" in call_args
    assert "score" in call_args


def test_transform_types_returns_original_when_no_table_name():
    """When metaschema is missing AND no table_name is provided,
    transform_types should return the original DataFrame unchanged."""
    mock_df = MagicMock()
    mock_df.columns = ["name", "age"]
    mock_df.schema.simpleString.return_value = "struct<name:string,age:int>"

    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark

    with patch(
        "src.utils.spark.get_schema_columns", side_effect=Exception("Table not found")
    ):
        df_out = transform_types(
            mock_df,
            schema_name="some_schema",
        )

    mock_spark.table.assert_not_called()
    mock_df.withColumns.assert_not_called()
    assert df_out == mock_df


def test_transform_types_returns_original_when_delta_table_missing():
    """When metaschema is missing AND the target Delta table also doesn't exist,
    transform_types should return the original DataFrame unchanged."""
    mock_df = MagicMock()
    mock_df.columns = ["name"]
    mock_df.schema.simpleString.return_value = "struct<name:string>"

    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_spark.table.side_effect = Exception("Delta table not found")

    with patch(
        "src.utils.spark.get_schema_columns",
        side_effect=Exception("Metaschema missing"),
    ):
        df_out = transform_types(
            mock_df,
            schema_name="giga_meter",
            table_name="connectivity_ping_checks",
        )

    mock_spark.table.assert_called_once_with("giga_meter.connectivity_ping_checks")
    mock_df.withColumns.assert_not_called()
    assert df_out == mock_df


def test_transform_types_uses_metaschema_when_available():
    """When metaschema IS available, transform_types should use it
    (no fallback needed)."""
    mock_df = MagicMock()
    mock_df.columns = ["name", "age"]
    mock_df.schema.simpleString.return_value = "struct<name:string,age:int>"

    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark

    mock_columns = [
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
    ]

    with (
        patch("src.utils.spark.get_schema_columns", return_value=mock_columns),
        patch("src.utils.spark.col", MagicMock()),
    ):
        transform_types(
            mock_df,
            schema_name="school_geolocation",
        )

    # Should NOT have queried the Delta table (metaschema was sufficient)
    mock_spark.table.assert_not_called()

    # Should have called withColumns to cast types
    assert mock_df.withColumns.called
