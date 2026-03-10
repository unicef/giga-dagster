from unittest.mock import MagicMock, patch

import pytest
from src.assets.unstructured.assets import (
    generalized_unstructured_raw,
    unstructured_raw,
)
from src.utils.op_config import DataTier, FileConfig


@pytest.fixture
def mock_config():
    return FileConfig(
        filepath="test/path/file.pdf",
        dataset_type="unstructured",
        country_code="BRA",
        destination_filepath="test/dest/file.pdf",
        metastore_schema="schema",
        tier=DataTier.RAW,
        file_size_bytes=100,
        metadata={},
        database_data="{}",
        dq_target_filepath="test/dq",
        domain="School",
        table_name="table",
    )


@patch("src.assets.unstructured.assets.log_op_context")
@patch("src.assets.unstructured.assets.datahub_emitter")
@patch("src.assets.unstructured.assets.define_dataset_properties")
@patch("src.assets.unstructured.assets.MetadataChangeProposalWrapper")
def test_unstructured_raw(
    mock_wrapper,
    mock_define_props,
    mock_emitter,
    mock_log_context,
    mock_config,
    op_context,
):
    mock_define_props.return_value = MagicMock()
    mock_wrapper.return_value = MagicMock()

    result = unstructured_raw(op_context, mock_config)

    assert result.value is None
    mock_emitter.emit.assert_called()
    mock_define_props.assert_called()


@patch("src.assets.unstructured.assets.log_op_context")
@patch("src.assets.unstructured.assets.datahub_emitter")
@patch("src.assets.unstructured.assets.define_dataset_properties")
@patch("src.assets.unstructured.assets.MetadataChangeProposalWrapper")
def test_generalized_unstructured_raw(
    mock_wrapper,
    mock_define_props,
    mock_emitter,
    mock_log_context,
    mock_config,
    op_context,
):
    mock_define_props.return_value = MagicMock()
    mock_wrapper.return_value = MagicMock()

    result = generalized_unstructured_raw(op_context, mock_config)

    assert result.value is None
    mock_emitter.emit.assert_called()
    mock_define_props.assert_called()


@patch("src.assets.unstructured.assets.log_op_context")
@patch("src.assets.unstructured.assets.datahub_emitter")
@patch("src.assets.unstructured.assets.define_dataset_properties")
def test_unstructured_raw_exception(
    mock_define_props, mock_emitter, mock_log_context, mock_config, op_context
):
    mock_define_props.side_effect = Exception("Test Error")

    result = unstructured_raw(op_context, mock_config)

    assert result.value is None
    mock_log_context.assert_called_with(op_context)


@patch("src.assets.unstructured.assets.log_op_context")
@patch("src.assets.unstructured.assets.datahub_emitter")
@patch("src.assets.unstructured.assets.define_dataset_properties")
def test_generalized_unstructured_raw_exception(
    mock_define_props, mock_emitter, mock_log_context, mock_config, op_context
):
    mock_define_props.side_effect = Exception("Test Error")

    result = generalized_unstructured_raw(op_context, mock_config)

    assert result.value is None
    mock_log_context.assert_called_with(op_context)
