from unittest.mock import MagicMock, patch

import pytest
from src.constants import DataTier
from src.resources.io_managers.adls_delta import ADLSDeltaIOManager

from dagster import InputContext, OutputContext


@pytest.fixture
def mock_settings():
    with patch("src.resources.io_managers.adls_delta.settings") as mock:
        mock.SPARK_WAREHOUSE_DIR = "/tmp/warehouse"
        mock.AZURE_BLOB_CONNECTION_URI = (
            "abfss://container@account.dfs.core.windows.net"
        )
        yield mock


@pytest.fixture
def mock_pyspark_resource():
    resource = MagicMock()
    resource.spark_session = MagicMock()
    return resource


@pytest.fixture
def manager(mock_pyspark_resource):
    try:
        from dagster_pyspark import PySparkResource

        resource = PySparkResource(spark_config={})
        manager = ADLSDeltaIOManager(pyspark=resource)
        return manager
    except Exception as e:
        pytest.fail(f"Failed to instantiate manager: {e}")


@patch("src.resources.io_managers.adls_delta.DeltaTable")
@patch("src.resources.io_managers.adls_delta.ADLSFileClient")
def test_handle_output_upsert(
    mock_adls_client, mock_delta_table, manager, mock_settings
):
    context = MagicMock(spec=OutputContext)
    context.step_context.op_config = {
        "metastore_schema": "school_master",
        "tier": DataTier.SILVER,
        "country_code": "BRA",
        "table_name": "schools",
        "filepath": "raw/schools.csv",
        "dataset_type": "geolocation",
        "file_size_bytes": 1000,
        "destination_filepath": "silver/schools",
    }

    output_df = MagicMock()
    output_df.isEmpty.return_value = False

    mock_delta_table.createIfNotExists.return_value.tableName.return_value.addColumns.return_value.property.return_value.property.return_value = MagicMock()

    with (
        patch(
            "src.resources.io_managers.adls_delta.get_schema_columns"
        ) as mock_get_cols,
        patch(
            "src.resources.io_managers.adls_delta.get_partition_columns"
        ) as mock_get_parts,
        patch("src.resources.io_managers.adls_delta.get_primary_key") as mock_get_pk,
        patch("src.resources.io_managers.adls_delta.execute_query_with_error_handler"),
        patch(
            "src.resources.io_managers.adls_delta.build_deduped_merge_query"
        ) as mock_build_merge,
    ):
        mock_get_cols.return_value = []
        mock_get_parts.return_value = []
        mock_get_pk.return_value = "id"

        mock_delta_table.forName.return_value.toDF.return_value.schema.fieldNames.return_value = [
            "col1"
        ]

        with patch(
            "src.resources.io_managers.adls_delta.ADLSDeltaIOManager._get_spark_session"
        ) as mock_get_spark:
            mock_spark = MagicMock()
            mock_get_spark.return_value = mock_spark

            manager.handle_output(context, output_df)

            mock_spark.sql.assert_called()

        mock_build_merge.assert_called()


@patch("src.resources.io_managers.adls_delta.DeltaTable")
def test_load_input(mock_delta_table, manager, mock_settings):
    context = MagicMock(spec=InputContext)
    context.step_context.op_config = {
        "metastore_schema": "school_master",
        "tier": DataTier.SILVER,
        "country_code": "BRA",
        "table_name": "schools",
        "filepath": "raw/schools.csv",
        "dataset_type": "geolocation",
        "file_size_bytes": 1000,
        "destination_filepath": "silver/schools",
    }

    context.upstream_output.step_context.op_config = {
        "metastore_schema": "school_master",
        "tier": DataTier.SILVER,
        "country_code": "BRA",
        "table_name": "schools",
        "filepath": "raw/schools.csv",
        "dataset_type": "geolocation",
        "file_size_bytes": 1000,
        "destination_filepath": "silver/schools",
    }

    mock_dt = mock_delta_table.forName.return_value

    with patch(
        "src.resources.io_managers.adls_delta.ADLSDeltaIOManager._get_spark_session"
    ) as mock_get_spark:
        mock_get_spark.return_value = MagicMock()

        df = manager.load_input(context)

    assert df == mock_dt.toDF.return_value
    mock_delta_table.forName.assert_called()


def test_handle_output_none(manager):
    context = MagicMock(spec=OutputContext)
    manager.handle_output(context, None)
    context.log.info.assert_called_with("Output is None, skipping execution.")
