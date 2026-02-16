from unittest.mock import MagicMock, patch

import pytest
from models import VALID_PRIMITIVES
from pyspark.sql.types import StringType, StructField, StructType
from src.assets.migrations.assets import initialize_metaschema, migrate_schema
from src.assets.migrations.core import (
    get_filepath,
    save_schema_delta_table,
    validate_raw_schema,
)


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.spark_session = spark
    spark.sql = MagicMock()
    spark.catalog = MagicMock()
    return spark


def test_get_filepath(mock_context):
    mock_context.run_tags = {"dagster/run_key": "path/to/file.csv:tag"}
    path = get_filepath(mock_context)
    assert path == "path/to/file.csv"


def test_validate_raw_schema_valid(spark_session, mock_context):
    mock_context.run_tags = {"dagster/run_key": "path.csv:tag"}
    schema = StructType([StructField("data_type", StringType(), True)])
    valid_val = VALID_PRIMITIVES[0]
    data = [(valid_val,)]
    df = spark_session.createDataFrame(data, schema)
    result_df = validate_raw_schema(mock_context, df)
    assert result_df.count() == 1


def test_validate_raw_schema_invalid(spark_session, mock_context):
    mock_context.run_tags = {"dagster/run_key": "path.csv:tag"}
    schema = StructType([StructField("data_type", StringType(), True)])
    data = [("INVALID_TYPE_XYZ",)]
    df = spark_session.createDataFrame(data, schema)
    with pytest.raises(ValueError) as excinfo:
        validate_raw_schema(mock_context, df)
    assert "Invalid data type found" in str(excinfo.value)


@patch("src.assets.migrations.core.execute_query_with_error_handler")
@patch("src.assets.migrations.core.DeltaTable")
def test_save_schema_delta_table(mock_delta_table, mock_exec, mock_context):
    mock_context.run_tags = {"dagster/run_key": "path/file.csv:tag"}
    mock_spark = MagicMock()
    df = MagicMock()
    df.sparkSession = mock_spark
    df.alias.return_value = df
    mock_delta_instance = mock_delta_table.return_value
    mock_delta_table.forName.return_value = mock_delta_instance
    mock_delta_instance.alias.return_value = mock_delta_instance
    mock_delta_instance.merge.return_value = mock_delta_instance
    mock_delta_instance.whenMatchedUpdateAll.return_value = mock_delta_instance
    mock_delta_instance.whenNotMatchedInsertAll.return_value = mock_delta_instance
    save_schema_delta_table(mock_context, df)
    mock_delta_table.createOrReplace.assert_called()
    mock_exec.assert_called()
    mock_delta_table.forName.assert_called()
    mock_spark.catalog.refreshTable.assert_called()


@pytest.mark.asyncio
async def test_initialize_metaschema(mock_spark, op_context):
    await initialize_metaschema(op_context, mock_spark)
    mock_spark.sql.assert_called()


@patch("src.assets.migrations.assets.save_schema_delta_table")
@patch("src.assets.migrations.assets.validate_raw_schema")
@patch("src.assets.migrations.assets.get_filepath")
@pytest.mark.asyncio
async def test_migrate_schema(
    mock_get_filepath,
    mock_validate,
    mock_save,
    mock_spark,
    mock_adls_client,
    op_context,
):
    mock_get_filepath.return_value = "raw/migrations/my-table.csv"
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_validate.return_value = mock_df
    mock_spark.catalog.isCached.return_value = False
    await migrate_schema(op_context, mock_adls_client, mock_spark)
    mock_adls_client.download_csv_as_pandas_dataframe.assert_called_with(
        "raw/migrations/my-table.csv"
    )
    mock_spark.createDataFrame.assert_called()
    mock_validate.assert_called()
    mock_save.assert_called()
    mock_spark.catalog.cacheTable.assert_called()
