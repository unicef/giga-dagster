from unittest.mock import MagicMock, patch

import pytest
from src.utils.delta import (
    build_nullability_queries,
    check_table_exists,
    create_delta_table,
    create_schema,
)


@pytest.fixture(autouse=True)
def patch_settings():
    with patch("src.utils.delta.settings") as mock_settings:
        mock_settings.SPARK_WAREHOUSE_DIR = "/tmp/warehouse"
        yield mock_settings


@pytest.fixture
def mock_spark():
    return MagicMock()


@patch("src.utils.delta.DeltaTable")
def test_create_delta_table(mock_delta_table, mock_spark):
    create_delta_table(
        mock_spark, "schema", "table", [], MagicMock(), if_not_exists=False
    )
    mock_delta_table.create.assert_called_with(mock_spark)

    create_delta_table(
        mock_spark, "schema", "table", [], MagicMock(), if_not_exists=True
    )
    mock_delta_table.createIfNotExists.assert_called_with(mock_spark)

    create_delta_table(mock_spark, "schema", "table", [], MagicMock(), replace=True)
    mock_delta_table.createOrReplace.assert_called_with(mock_spark)


def test_check_table_exists(mock_spark):
    mock_spark.catalog.tableExists.return_value = True

    with patch("src.utils.delta.DeltaTable.isDeltaTable") as mock_is_delta:
        mock_is_delta.return_value = True

        exists = check_table_exists(mock_spark, "schema", "table")
        assert exists is True

        mock_is_delta.return_value = False
        assert check_table_exists(mock_spark, "schema", "table") is False


def test_create_schema(mock_spark):
    create_schema(mock_spark, "new_schema")
    mock_spark.sql.assert_called_with("CREATE SCHEMA IF NOT EXISTS `new_schema`")


def test_build_nullability_queries():
    context = MagicMock()
    existing_schema = MagicMock()
    updated_schema = MagicMock()

    f1 = MagicMock()
    f1.name = "col1"
    f1.nullable = False
    f2 = MagicMock()
    f2.name = "col1"
    f2.nullable = True

    existing_schema.__iter__.return_value = [f1]
    updated_schema.__iter__.return_value = [f2]

    existing_list = [f1]
    updated_list = [f2]

    stmts = build_nullability_queries(
        context, existing_list, updated_list, "table_name"
    )

    assert len(stmts) == 2
    assert "DROP CONSTRAINT IF EXISTS col1_not_null" in stmts[0]
    assert len(stmts) == 2
    assert "DROP CONSTRAINT IF EXISTS col1_not_null" in stmts[0]
    assert "DROP NOT NULL" in stmts[1]


def test_get_changed_datatypes():
    from pyspark.sql.types import IntegerType, StringType
    from src.utils.delta import get_changed_datatypes

    context = MagicMock()
    existing_schema = [MagicMock(name="col1", dataType=IntegerType())]
    updated_schema = [MagicMock(name="col1", dataType=StringType())]

    existing_schema[0].name = "col1"
    updated_schema[0].name = "col1"

    diff = get_changed_datatypes(context, existing_schema, updated_schema)
    assert diff["col1"] == StringType()


def test_sync_schema(mock_spark):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType
    from src.utils.delta import sync_schema

    context = MagicMock()
    existing_schema = StructType([StructField("col1", IntegerType())])
    updated_schema = StructType(
        [StructField("col1", StringType()), StructField("col2", StringType())]
    )

    df_mock = MagicMock()
    mock_spark.table.return_value = df_mock
    df_mock.withColumn.return_value = df_mock

    write_mock = MagicMock()
    df_mock.write = write_mock
    write_mock.option.return_value = write_mock
    write_mock.format.return_value = write_mock
    write_mock.mode.return_value = write_mock

    mock_spark.createDataFrame.return_value = df_mock

    sync_schema("table", existing_schema, updated_schema, mock_spark, context)

    assert mock_spark.table.called
    df_mock.withColumn.assert_called()
    write_mock.saveAsTable.assert_called_with("table")

    mock_spark.createDataFrame.assert_called()
