from unittest.mock import MagicMock, patch

from pyspark.sql.types import DoubleType, StringType
from src.constants import DataTier
from src.utils.schema import (
    construct_schema_name_for_tier,
    get_schema_columns,
    get_schema_name,
)

from dagster import OpExecutionContext


@patch("src.utils.schema.get_schema_table")
def test_get_schema_columns(mock_get_table):
    spark = MagicMock()
    mock_row = MagicMock()
    mock_row.name = "col1"
    mock_row.data_type = "string"
    mock_row.is_nullable = True
    mock_get_table.return_value.collect.return_value = [mock_row]
    with patch("src.utils.schema.constants.TYPE_MAPPINGS") as mock_mapping:
        mock_mapping.string.pyspark.return_value = StringType()
        cols = get_schema_columns(spark, "schema")
        assert len(cols) == 1
        assert cols[0].name == "col1"
        assert isinstance(cols[0].dataType, StringType)


def test_construct_schema_name_for_tier():
    assert construct_schema_name_for_tier("School", DataTier.SILVER) == "school_silver"
    assert construct_schema_name_for_tier("School", DataTier.RAW) == "school"


def test_get_schema_name():
    context = MagicMock(spec=OpExecutionContext)
    context.op_config = {"metastore_schema": "test_schema"}
    assert get_schema_name(context) == "test_schema"


@patch("src.utils.schema.get_schema_table")
def test_get_schema_columns_school_geolocation_adds_core_fields(mock_get_table):
    """Test that school_geolocation schema adds missing core fields."""
    spark = MagicMock()
    mock_row = MagicMock()
    mock_row.name = "col1"
    mock_row.data_type = "string"
    mock_row.is_nullable = True
    mock_get_table.return_value.collect.return_value = [mock_row]

    with patch("src.utils.schema.constants.TYPE_MAPPINGS") as mock_mapping:
        mock_mapping.string.pyspark.return_value = StringType()
        mock_mapping.double.pyspark.return_value = DoubleType()

        cols = get_schema_columns(spark, "school_geolocation_silver")

    # col1 + 5 core fields (school_id_govt, school_name, education_level_govt, latitude, longitude)
    assert len(cols) == 6
    col_names = [c.name for c in cols]
    assert "school_id_govt" in col_names
    assert "latitude" in col_names
    assert "longitude" in col_names


@patch("src.utils.schema.get_schema_table")
def test_get_schema_columns_school_geolocation_no_duplicates(mock_get_table):
    """Test that existing core fields are not duplicated."""
    spark = MagicMock()
    mock_row = MagicMock()
    mock_row.name = "school_id_govt"
    mock_row.data_type = "string"
    mock_row.is_nullable = False
    mock_get_table.return_value.collect.return_value = [mock_row]

    with patch("src.utils.schema.constants.TYPE_MAPPINGS") as mock_mapping:
        mock_mapping.string.pyspark.return_value = StringType()
        mock_mapping.double.pyspark.return_value = DoubleType()

        cols = get_schema_columns(spark, "school_geolocation_bronze")

    # school_id_govt already present, so only 4 new ones added
    col_names = [c.name for c in cols]
    assert col_names.count("school_id_govt") == 1
