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


# ---------------------------------------------------------------------------
# unstructured_raw  –  happy path: emits metadata to DataHub
# ---------------------------------------------------------------------------
@patch("src.assets.unstructured.assets.get_output_metadata", return_value={})
@patch("src.assets.unstructured.assets.log_op_context")
@patch("src.assets.unstructured.assets.get_datahub_emitter")
@patch("src.assets.unstructured.assets.define_dataset_properties")
@patch("src.assets.unstructured.assets.MetadataChangeProposalWrapper")
def test_unstructured_raw(
    mock_wrapper,
    mock_define_props,
    mock_get_emitter,
    mock_log_context,
    mock_get_metadata,
    mock_config,
    op_context,
):
    mock_emitter = MagicMock()
    mock_get_emitter.return_value = mock_emitter
    mock_define_props.return_value = MagicMock()
    mock_wrapper.return_value = MagicMock()

    result = unstructured_raw(op_context, mock_config)

    assert result.value is None
    mock_emitter.emit.assert_called()
    mock_define_props.assert_called()


# ---------------------------------------------------------------------------
# generalized_unstructured_raw  –  happy path
# ---------------------------------------------------------------------------
@patch("src.assets.unstructured.assets.get_output_metadata", return_value={})
@patch("src.assets.unstructured.assets.log_op_context")
@patch("src.assets.unstructured.assets.get_datahub_emitter")
@patch("src.assets.unstructured.assets.define_dataset_properties")
@patch("src.assets.unstructured.assets.MetadataChangeProposalWrapper")
def test_generalized_unstructured_raw(
    mock_wrapper,
    mock_define_props,
    mock_get_emitter,
    mock_log_context,
    mock_get_metadata,
    mock_config,
    op_context,
):
    mock_emitter = MagicMock()
    mock_get_emitter.return_value = mock_emitter
    mock_define_props.return_value = MagicMock()
    mock_wrapper.return_value = MagicMock()

    result = generalized_unstructured_raw(op_context, mock_config)

    assert result.value is None
    mock_emitter.emit.assert_called()
    mock_define_props.assert_called()


# ---------------------------------------------------------------------------
# unstructured_raw  –  exception path: define_dataset_properties raises
# ---------------------------------------------------------------------------
@patch("src.assets.unstructured.assets.get_output_metadata", return_value={})
@patch("src.assets.unstructured.assets.log_op_context")
@patch("src.assets.unstructured.assets.get_datahub_emitter")
@patch("src.assets.unstructured.assets.define_dataset_properties")
def test_unstructured_raw_exception(
    mock_define_props,
    mock_get_emitter,
    mock_log_context,
    mock_get_metadata,
    mock_config,
    op_context,
):
    mock_emitter = MagicMock()
    mock_get_emitter.return_value = mock_emitter
    mock_define_props.side_effect = Exception("Test Error")

    result = unstructured_raw(op_context, mock_config)

    assert result.value is None
    mock_log_context.assert_called_with(op_context)


# ---------------------------------------------------------------------------
# generalized_unstructured_raw  –  exception path
# ---------------------------------------------------------------------------
@patch("src.assets.unstructured.assets.get_output_metadata", return_value={})
@patch("src.assets.unstructured.assets.log_op_context")
@patch("src.assets.unstructured.assets.get_datahub_emitter")
@patch("src.assets.unstructured.assets.define_dataset_properties")
def test_generalized_unstructured_raw_exception(
    mock_define_props,
    mock_get_emitter,
    mock_log_context,
    mock_get_metadata,
    mock_config,
    op_context,
):
    mock_emitter = MagicMock()
    mock_get_emitter.return_value = mock_emitter
    mock_define_props.side_effect = Exception("Test Error")

    result = generalized_unstructured_raw(op_context, mock_config)

    assert result.value is None
    mock_log_context.assert_called_with(op_context)


# ---------------------------------------------------------------------------
# unstructured_raw  –  no DataHub emitter configured
# ---------------------------------------------------------------------------
@patch("src.assets.unstructured.assets.get_output_metadata", return_value={})
@patch("src.assets.unstructured.assets.get_datahub_emitter", return_value=None)
def test_unstructured_raw_no_datahub(
    mock_get_emitter, mock_get_metadata, mock_config, op_context
):
    result = unstructured_raw(op_context, mock_config)

    assert result.value is None
