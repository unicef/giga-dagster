from unittest.mock import patch

from src.utils.datahub.update_policies import (
    list_datasets_by_filter,
    update_policies,
    update_policy_base,
    update_policy_for_group,
)
from src.utils.op_config import DataTier, FileConfig


@patch("src.utils.datahub.update_policies.datahub_graph_client")
@patch("src.utils.datahub.update_policies.identify_country_name")
@patch("src.utils.datahub.update_policies.build_group_urn")
@patch("src.utils.datahub.update_policies.is_valid_country_name")
def test_update_policy_for_group(
    mock_is_valid, mock_build_urn, mock_identify, mock_graph, mock_context
):
    mock_identify.return_value = "Brazil"
    mock_build_urn.return_value = "urn:li:corpGroup:Brazil-Master%20Table"
    mock_is_valid.return_value = True

    mock_graph.get_urns_by_filter.return_value = ["urn:li:dataset:1"]

    config = FileConfig(
        filepath="/file.csv",
        dataset_type="master",
        destination_filepath="/dest",
        file_size_bytes=100,
        country_code="BRA",
        metastore_schema="schema",
        tier=DataTier.RAW,
    )

    update_policy_for_group(config, mock_context)

    mock_graph.execute_graphql.assert_called()
    assert "updatePolicy" in mock_graph.execute_graphql.call_args[1]["query"]


@patch("src.utils.datahub.update_policies.datahub_graph_client")
@patch("src.utils.datahub.update_policies.is_valid_country_name")
def test_update_policy_base_invalid(mock_is_valid, mock_graph):
    mock_is_valid.return_value = False

    update_policy_base("urn:li:corpGroup:Invalid-master")

    mock_graph.execute_graphql.assert_not_called()


@patch("src.utils.datahub.update_policies.group_urns_iterator")
@patch("src.utils.datahub.update_policies.execute_batch_mutation")
@patch("src.utils.datahub.update_policies.is_valid_country_name")
def test_update_policies_batch(mock_is_valid, mock_batch, mock_iterator, mock_context):
    mock_iterator.return_value = [
        "urn:li:corpGroup:Brazil-Master%20Table",
        "urn:li:corpGroup:Rwanda-Master%20Table",
    ]
    mock_is_valid.return_value = True

    with patch(
        "src.utils.datahub.update_policies.list_datasets_by_filter",
        return_value='["urn1"]',
    ):
        update_policies(mock_context)

    mock_batch.assert_called()


@patch("src.utils.datahub.update_policies.datahub_graph_client")
def test_list_datasets_by_filter(mock_graph):
    mock_graph.get_urns_by_filter.return_value = ["urn1", "urn2"]

    res = list_datasets_by_filter("Brazil", "master")
    assert '"urn1"' in res
    assert '"urn2"' in res
