from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import IntegerType, StringType
from src.utils.spark import (
    compute_row_hash,
    get_spark_session,
    transform_columns,
    transform_types,
)


@pytest.fixture
def mock_settings():
    with patch("src.utils.spark.settings") as mock:
        mock.SPARK_WAREHOUSE_DIR = "/tmp"
        mock.HIVE_METASTORE_URI = "thrift://localhost:9083"
        mock.SPARK_DRIVER_CORES = 1
        mock.SPARK_DRIVER_MEMORY_MB = 1024
        mock.AZURE_BLOB_CONTAINER_NAME = "container"
        mock.AZURE_STORAGE_ACCOUNT_NAME = "account"
        mock.AZURE_SAS_TOKEN = "token"
        mock.COMMIT_SHA = "sha"
        mock.IN_PRODUCTION = False
        yield mock


def test_get_spark_session(mock_settings):
    with (
        patch("src.utils.spark.SparkSession") as _,
        patch("src.utils.spark.configure_spark_with_delta_pip") as mock_conf_delta,
        patch("src.utils.spark.subprocess.run"),
    ):
        mock_conf_delta.return_value.getOrCreate.return_value = "session"
        assert get_spark_session() == "session"


def test_transform_columns(spark_session):
    data = [(1, "a"), (2, "b")]
    df = spark_session.createDataFrame(data, ["col1", "col2"])
    df_new = transform_columns(df, ["col1"], StringType())
    dtype = dict(df_new.dtypes)["col1"]
    assert dtype == "string"
    assert df_new.count() == 2


@patch("src.utils.spark.get_schema_columns")
def test_transform_types(mock_get_schema, spark_session):
    data = [("1", "a")]
    df = spark_session.createDataFrame(data, ["id", "val"])
    mock_field = MagicMock()
    mock_field.name = "id"
    mock_field.dataType = IntegerType()
    mock_get_schema.return_value = [mock_field]
    context = MagicMock()
    df_new = transform_types(df, "test_schema", context)
    dtype = dict(df_new.dtypes)["id"]
    assert dtype == "int"


def test_compute_row_hash(spark_session):
    data = [("1", "a"), (2, "b")]
    df = spark_session.createDataFrame(data, ["c1", "c2"])
    df_hash = compute_row_hash(df)
    assert "signature" in df_hash.columns
    row = df_hash.collect()[0]
    assert isinstance(row["signature"], str)
    assert len(row["signature"]) == 64
