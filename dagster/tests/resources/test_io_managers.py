from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.resources.io_managers.adls_generic_file import ADLSGenericFileIOManager
from src.resources.io_managers.adls_json import ADLSJSONIOManager
from src.resources.io_managers.adls_pandas import ADLSPandasIOManager
from src.resources.io_managers.adls_passthrough import ADLSPassthroughIOManager

from dagster import build_input_context, build_output_context


@pytest.fixture
def io_manager_adls_mocks():
    with (
        patch("src.resources.io_managers.adls_generic_file.adls_client") as m1,
        patch("src.resources.io_managers.adls_json.adls_client") as m2,
        patch("src.resources.io_managers.adls_pandas.adls_client") as m3,
        patch("src.resources.io_managers.adls_passthrough.adls_client") as m4,
    ):
        yield m1, m2, m3, m4


from pathlib import Path


def test_generic_file_io_manager(io_manager_adls_mocks):
    mock_client = io_manager_adls_mocks[0]
    manager = ADLSGenericFileIOManager()

    context = build_output_context(name="test_asset", metadata={"key": "value"})

    with patch.object(
        ADLSGenericFileIOManager, "_get_filepath", return_value=Path("path/to/file.txt")
    ):
        manager.handle_output(context, b"data")
        mock_client.upload_raw.assert_called()

    input_context = build_input_context(name="test_asset")
    mock_client.download_raw.return_value = b"data"
    with patch.object(
        ADLSGenericFileIOManager, "_get_filepath", return_value=Path("path/to/file.txt")
    ):
        res = manager.load_input(input_context)
        assert res == b"data"


def test_json_io_manager(io_manager_adls_mocks):
    mock_client = io_manager_adls_mocks[1]
    manager = ADLSJSONIOManager()
    data = {"key": "value"}

    with patch.object(
        ADLSJSONIOManager, "_get_filepath", return_value=Path("path/to/file.json")
    ):
        out_ctx = build_output_context(name="asset")
        manager.handle_output(out_ctx, data)
        mock_client.upload_json.assert_called_with(data, "path/to/file.json")

        in_ctx = build_input_context(name="asset")
        mock_client.download_json.return_value = data
        res = manager.load_input(in_ctx)
        assert res == data


from dagster_pyspark import PySparkResource


def test_pandas_io_manager(io_manager_adls_mocks):
    mock_client = io_manager_adls_mocks[2]

    spark_resource = PySparkResource(spark_config={"spark.master": "local[1]"})

    df = pd.DataFrame({"a": [1]})

    with patch.object(
        ADLSPandasIOManager, "_get_filepath", return_value=Path("path/to/file.csv")
    ):
        pass

    manager = ADLSPandasIOManager.construct(pyspark=spark_resource)

    with patch.object(
        ADLSPandasIOManager, "_get_filepath", return_value=Path("path/to/file.csv")
    ):
        manager.handle_output(build_output_context(name="asset"), df)
        mock_client.upload_pandas_dataframe_as_file.assert_called()

        pass


def test_pandas_io_manager_fixed(io_manager_adls_mocks):
    mock_client = io_manager_adls_mocks[2]
    mock_pyspark = MagicMock()
    mock_pyspark.spark_session = MagicMock()

    manager = ADLSPandasIOManager.construct(pyspark=mock_pyspark)

    df = pd.DataFrame({"a": [1]})

    with patch.object(
        ADLSPandasIOManager, "_get_filepath", return_value=Path("path/to/file.csv")
    ):
        manager.handle_output(build_output_context(name="asset"), df)
        mock_client.upload_pandas_dataframe_as_file.assert_called()

        manager.load_input(build_input_context(name="asset"))
        mock_client.download_csv_as_spark_dataframe.assert_called()


def test_passthrough_io_manager(io_manager_adls_mocks):
    mock_client = io_manager_adls_mocks[3]
    manager = ADLSPassthroughIOManager()

    with patch.object(
        ADLSPassthroughIOManager, "_get_filepath", return_value=Path("path/to/file")
    ):
        out_ctx = build_output_context(name="asset")
        manager.handle_output(out_ctx, b"ignored")

        in_ctx = build_input_context(name="asset")
        mock_client.download_raw.return_value = b"data"
        res = manager.load_input(in_ctx)
        assert res == b"data"
