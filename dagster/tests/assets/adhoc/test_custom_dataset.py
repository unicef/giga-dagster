from src.assets.adhoc.custom_dataset import custom_dataset_raw
from src.constants import DataTier
from src.utils.op_config import FileConfig


def test_custom_dataset_raw_downloads_file(spark_session, mock_adls_client, op_context):
    mock_adls_client.download_raw.return_value = b"test,data\n1,2\n"
    config = FileConfig(
        filepath="custom_data/TEST/file.csv",
        dataset_type="custom",
        tier=DataTier.RAW,
        country_code="TEST",
        destination_filepath="",
        file_size_bytes=10,
        metastore_schema="custom",
        domain="custom",
        table_name="table",
    )
    result = custom_dataset_raw(op_context, mock_adls_client, config)
    assert result.value == b"test,data\n1,2\n"
    assert result.metadata is not None
    mock_adls_client.download_raw.assert_called_once_with("custom_data/TEST/file.csv")
