from unittest.mock import MagicMock, patch

from pyspark.sql.types import StringType
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
