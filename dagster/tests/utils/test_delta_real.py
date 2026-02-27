from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType
from src.exceptions import MutexException
from src.utils.delta import (
    check_table_exists,
    create_delta_table,
    get_change_operation_counts,
    sync_schema,
)


@patch("src.utils.delta.DeltaTable")
def test_check_table_exists(mock_delta_cls):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    mock_delta_cls.isDeltaTable.return_value = True
    with patch("src.utils.delta.settings") as mock_settings:
        mock_settings.SPARK_WAREHOUSE_DIR = "/warehouse"
        assert check_table_exists(spark, "schema", "table") is True
    spark.catalog.tableExists.return_value = False
    assert check_table_exists(spark, "schema", "table") is False


@patch("src.utils.delta.DeltaTable")
def test_create_delta_table(mock_delta_cls):
    spark = MagicMock()
    context = MagicMock()
    create_delta_table(spark, "schema", "table", [], context)
    mock_delta_cls.create.return_value.tableName.return_value.addColumns.return_value.property.return_value.execute.assert_called()
    with pytest.raises(MutexException):
        create_delta_table(
            spark, "schema", "table", [], context, if_not_exists=True, replace=True
        )


def test_get_change_operation_counts(spark_session):
    data = [("insert",), ("insert",), ("delete",)]
    df = spark_session.createDataFrame(data, ["_change_type"])
    counts = get_change_operation_counts(df)
    assert counts["added"] == 2
    assert counts["deleted"] == 1
    assert counts["modified"] == 0


@patch("src.utils.delta.build_nullability_queries")
@patch("src.utils.delta.get_changed_datatypes")
def test_sync_schema_updates(mock_get_types, mock_null_queries, spark_session):
    schema1 = StructType([StructField("col1", StringType())])
    schema2 = StructType([StructField("col1", StringType())])
    context = MagicMock()
    spark = MagicMock()
    mock_null_queries.return_value = []
    mock_get_types.return_value = {}
    sync_schema("table", schema1, schema2, spark, context)
    context.log.info.assert_called()
