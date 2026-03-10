from unittest.mock import MagicMock, patch

import pytest
from src.utils.datahub.entity import (
    delete_entity_with_references,
    get_entity_count_safe,
)

from dagster import OpExecutionContext


@pytest.fixture
def mock_context():
    context = MagicMock(spec=OpExecutionContext)
    context.log = MagicMock()
    return context


@patch("src.utils.datahub.entity.datahub_graph_client")
def test_delete_entity_with_references_soft(mock_graph, mock_context):
    mock_graph.delete_references_to_urn.return_value = (5, [])
    urn = "urn:li:dataset:test"

    count = delete_entity_with_references(mock_context, urn, hard_delete=False)

    assert count == 5
    mock_graph.delete_references_to_urn.assert_called_with(urn=urn, dry_run=False)
    mock_graph.soft_delete_entity.assert_called_with(urn=urn)
    mock_graph.hard_delete_entity.assert_not_called()
    mock_context.log.info.assert_called_with(f"Deleted 5 references to {urn}")


@patch("src.utils.datahub.entity.datahub_graph_client")
def test_delete_entity_with_references_hard(mock_graph, mock_context):
    mock_graph.delete_references_to_urn.return_value = (0, [])
    urn = "urn:li:dataset:test"

    count = delete_entity_with_references(mock_context, urn, hard_delete=True)

    assert count == 0
    mock_graph.delete_references_to_urn.assert_called_with(urn=urn, dry_run=False)
    mock_graph.hard_delete_entity.assert_called_with(urn=urn)
    mock_graph.soft_delete_entity.assert_not_called()

    mock_context.log.info.assert_not_called()


@patch("src.utils.datahub.entity.datahub_graph_client")
def test_get_entity_count_safe_pagination(mock_graph):
    batch_size = 100
    mock_graph.list_all_entity_urns.side_effect = [
        ["urn"] * batch_size,
        ["urn"] * batch_size,
        ["urn"] * 50,
    ]

    total = get_entity_count_safe(entity_type="dataset", batch_size=batch_size)

    assert total == 250
    assert mock_graph.list_all_entity_urns.call_count == 3
    # Check calls
    mock_graph.list_all_entity_urns.assert_any_call(
        entity_type="dataset", start=0, count=batch_size
    )
    mock_graph.list_all_entity_urns.assert_any_call(
        entity_type="dataset", start=100, count=batch_size
    )
    mock_graph.list_all_entity_urns.assert_any_call(
        entity_type="dataset", start=200, count=batch_size
    )


@patch("src.utils.datahub.entity.datahub_graph_client")
def test_get_entity_count_safe_retry_success(mock_graph):
    mock_graph.list_all_entity_urns.side_effect = [
        Exception("Timeout"),
        ["urn"] * 50,
        ["urn"] * 10,
    ]

    total = get_entity_count_safe(entity_type="dataset", batch_size=100)

    assert total == 60
    mock_graph.list_all_entity_urns.assert_any_call(
        entity_type="dataset", start=0, count=100
    )
    mock_graph.list_all_entity_urns.assert_any_call(
        entity_type="dataset", start=0, count=50
    )
    mock_graph.list_all_entity_urns.assert_any_call(
        entity_type="dataset", start=50, count=50
    )


@patch("src.utils.datahub.entity.datahub_graph_client")
def test_get_entity_count_safe_retry_failure(mock_graph, capsys):
    mock_graph.list_all_entity_urns.side_effect = Exception("Persistent Fail")

    total = get_entity_count_safe(entity_type="dataset", batch_size=20)

    assert total == 0
    captured = capsys.readouterr()
    assert "Failed even with smallest batch size" in captured.out


@patch("src.utils.datahub.entity.datahub_graph_client")
def test_get_entity_count_safe_safety_limit(mock_graph, capsys):
    large_batch = 100000

    mock_graph.list_all_entity_urns.return_value = ["urn"] * large_batch

    total = get_entity_count_safe(entity_type="dataset", batch_size=large_batch)

    assert total == 6 * large_batch
    captured = capsys.readouterr()
    assert "Reached safety limit" in captured.out
