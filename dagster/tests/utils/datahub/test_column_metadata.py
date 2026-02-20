from unittest.mock import MagicMock, patch

import pytest
from src.utils.datahub.column_metadata import (
    add_column_metadata,
    get_column_licenses,
)


@pytest.fixture
def mock_db_context():
    with patch("src.utils.datahub.column_metadata.get_db_context") as mock_db:
        mock_session = MagicMock()
        mock_db.return_value.__enter__.return_value = mock_session
        yield mock_session


def test_get_column_licenses(mock_db_context):
    mock_config = MagicMock()
    mock_config.filename_components.id = 1

    mock_upload = MagicMock()

    with patch("src.utils.datahub.column_metadata.FileUploadConfig") as MockConfigCls:
        MockConfigCls.from_orm.return_value.column_license = {"col1": "lic1"}

        mock_db_context.scalar.return_value = mock_upload

        res = get_column_licenses(mock_config)
        assert res == {"col1": "lic1"}


def test_add_column_metadata_licenses(mock_context):
    dataset_urn = "urn:li:dataset:test"
    licenses = {"col1": "lic1"}

    with (
        patch("src.utils.datahub.column_metadata.datahub_graph_client") as mock_client,
        patch("src.utils.datahub.column_metadata.execute_batch_mutation") as mock_exec,
    ):
        mock_field = MagicMock()
        mock_field.fieldPath = "col1"
        mock_client.get_schema_metadata.return_value.fields = [mock_field]

        add_column_metadata(dataset_urn, column_licenses=licenses, context=mock_context)

        assert mock_exec.called
        assert "addTag" in mock_exec.call_args[0][0]
        assert "urn:li:tag:lic1" in mock_exec.call_args[0][0]


def test_add_column_metadata_descriptions(mock_context):
    dataset_urn = "urn:li:dataset:test"
    descriptions = {"col1": "desc1"}

    with (
        patch("src.utils.datahub.column_metadata.datahub_graph_client") as mock_client,
        patch("src.utils.datahub.column_metadata.execute_batch_mutation") as mock_exec,
    ):
        mock_field = MagicMock()
        mock_field.fieldPath = "col1"
        mock_client.get_schema_metadata.return_value.fields = [mock_field]

        add_column_metadata(
            dataset_urn, column_descriptions=descriptions, context=mock_context
        )

        assert mock_exec.called
        assert "updateDescription" in mock_exec.call_args[0][0]
        assert "desc1" in mock_exec.call_args[0][0]
