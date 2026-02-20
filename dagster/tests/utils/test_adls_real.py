from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.utils.adls import ADLSFileClient

from dagster import OpExecutionContext


@pytest.fixture
def mock_adls_service():
    with patch("src.utils.adls._adls") as mock:
        yield mock


def test_get_metadata_path():
    assert (
        ADLSFileClient._get_metadata_path("folder/file.csv")
        == "folder/file.metadata.json"
    )
    with (
        patch("src.constants.constants_class.constants.UPLOAD_PATH_PREFIX", "uploads"),
        patch(
            "src.constants.constants_class.constants.UPLOAD_METADATA_PATH_PREFIX",
            "metadata",
        ),
    ):
        path = ADLSFileClient._get_metadata_path("uploads/file.csv")
        assert path == "metadata/file.csv.metadata.json"


def test_download_raw(mock_adls_service):
    mock_file_client = MagicMock()
    mock_file_client.download_file.return_value.readinto.side_effect = (
        lambda buffer: buffer.write(b"content")
    )
    mock_adls_service.get_file_client.return_value = mock_file_client
    result = ADLSFileClient.download_raw("path/to/file.txt")
    assert result == b"content"
    mock_adls_service.get_file_client.assert_called_with("path/to/file.txt")


def test_upload_raw(mock_adls_service):
    mock_file_client = MagicMock()
    mock_adls_service.get_file_client.return_value = mock_file_client
    mock_context = MagicMock()
    mock_context.step_context.op_config = {"metadata": {"key": "value"}}
    ADLSFileClient.upload_raw(mock_context, b"data", "path/file.txt")
    mock_adls_service.get_file_client.assert_any_call("path/file.txt")
    mock_adls_service.get_file_client.assert_any_call("path/file.metadata.json")
    assert mock_file_client.upload_data.call_count >= 2


def test_download_csv_as_pandas_dataframe(mock_adls_service):
    csv_content = b"col1,col2\n1,a\n2,b"
    mock_file_client = MagicMock()

    def side_effect(buffer):
        buffer.write(csv_content)
        return len(csv_content)

    mock_file_client.download_file.return_value.readinto.side_effect = side_effect
    mock_adls_service.get_file_client.return_value = mock_file_client
    client = ADLSFileClient()
    df = client.download_csv_as_pandas_dataframe("file.csv")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "col1" in df.columns


def test_fetch_metadata_for_blob(mock_adls_service):
    client = ADLSFileClient()
    with patch(
        "src.utils.adls.ADLSFileClient.download_json", return_value={"sidecar": "true"}
    ):
        metadata = client.fetch_metadata_for_blob("file.txt")
        assert metadata == {"sidecar": "true"}
    file_props_mock = MagicMock()
    file_props_mock.metadata = {"blob_prop": "true"}
    with (
        patch("src.utils.adls.ADLSFileClient.download_json", return_value=None),
        patch(
            "src.utils.adls.ADLSFileClient.get_file_metadata",
            return_value=file_props_mock,
        ),
    ):
        metadata = client.fetch_metadata_for_blob("file.txt")
        assert metadata == {"blob_prop": "true"}


def test_exists(mock_adls_service):
    client = ADLSFileClient()
    mock_adls_service.get_file_client.return_value.exists.return_value.exists.return_value = True
    assert client.exists("path") is True


def test_download_csv_as_spark_dataframe(mock_adls_service):
    mock_spark = MagicMock()
    mock_read = MagicMock()
    mock_spark.read.csv.return_value = mock_read
    mock_read.columns = ["col1", "col2"]

    client = ADLSFileClient()
    df = client.download_csv_as_spark_dataframe("file.csv", mock_spark)

    assert df == mock_read
    mock_spark.read.csv.assert_called()


def test_upload_pandas_dataframe_as_file(mock_adls_service):
    mock_file_client = MagicMock()
    mock_adls_service.get_file_client.return_value = mock_file_client

    df = pd.DataFrame({"col": [1, 2]})
    client = ADLSFileClient()

    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context._step_execution_context.op_config = {
        "metadata": {"key": "val", "dict_key": {"inner": "val"}},
        "filepath": "/path/to/file.csv",
        "dataset_type": "master",
        "country_code": "BRA",
        "destination_filepath": "dest.csv",
        "metastore_schema": "schema",
        "file_size_bytes": 100,
        "tier": "raw",
    }

    client.upload_pandas_dataframe_as_file(mock_context, df, "test.csv")

    assert mock_file_client.upload_data.call_count >= 2


def test_list_paths(mock_adls_service):
    client = ADLSFileClient()
    mock_adls_service.get_paths.return_value = ["path1", "path2"]
    paths = client.list_paths("folder")
    assert paths == ["path1", "path2"]


def test_rename_file(mock_adls_service):
    client = ADLSFileClient()
    mock_file_client = MagicMock()
    mock_adls_service.get_file_client.return_value = mock_file_client
    mock_file_client.file_system_name = "fs"

    client.rename_file("old.txt", "new.txt")
    mock_file_client.rename_file.assert_called()


def test_delete_file(mock_adls_service):
    mock_file_client = MagicMock()
    mock_adls_service.get_file_client.return_value = mock_file_client

    ADLSFileClient.delete("file.txt")
    mock_file_client.delete_file.assert_called_with(recursive=False)


def test_folder_exists(mock_adls_service):
    with patch("src.utils.adls._client") as mock_client_global:
        mock_fs = MagicMock()
        mock_client_global.get_file_system_client.return_value = mock_fs

        client = ADLSFileClient()
        assert client.folder_exists("folder") is True
