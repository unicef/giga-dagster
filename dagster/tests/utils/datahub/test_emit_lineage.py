from unittest.mock import patch

import pytest
from src.utils.datahub.emit_lineage import (
    emit_lineage,
    emit_lineage_base,
    emit_lineage_query,
)


@pytest.fixture
def mock_graph_client():
    with patch("src.utils.datahub.emit_lineage.datahub_graph_client") as mock:
        yield mock


def test_emit_lineage_query(mock_context, mock_graph_client):
    emit_lineage_query("urn:upstream", "urn:downstream", mock_context)
    assert mock_graph_client.execute_graphql.called
    args = mock_graph_client.execute_graphql.call_args[1]
    assert "urn:upstream" in args["query"]
    assert "urn:downstream" in args["query"]


def test_emit_lineage_query_error(mock_context, mock_graph_client):
    mock_graph_client.execute_graphql.side_effect = Exception("GraphQLError")
    with patch("src.utils.datahub.emit_lineage.sentry_sdk") as mock_sentry:
        emit_lineage_query("urn:u", "urn:d", mock_context)
        assert mock_sentry.capture_exception.called


def test_emit_lineage_base(mock_context, mock_graph_client):
    upstreams = ["urn:li:dataset:1", "path/to/file.csv"]
    downstream = "urn:li:dataset:2"

    with patch("src.utils.datahub.emit_lineage.build_dataset_urn") as mock_build:
        mock_build.return_value = "urn:li:dataset:file"

        emit_lineage_base(upstreams, downstream, mock_context)

        assert mock_graph_client.execute_graphql.call_count == 2


def test_emit_lineage_op(mock_context, mock_graph_client):
    mock_context.asset_key.to_user_string.return_value = "step_name"
    mock_context.get_step_execution_context.return_value.op_config = {
        "filename_components": {
            "id": 1,
            "country_code": "BRA",
            "filename_date_str": "2024",
        },
    }

    with patch("src.utils.datahub.emit_lineage.FileConfig") as MockConfig:
        instance = MockConfig.return_value
        instance.datahub_source_dataset_urn = "urn:source"
        instance.datahub_destination_dataset_urn = "urn:dest"

        emit_lineage(mock_context)

        assert mock_graph_client.execute_graphql.called
