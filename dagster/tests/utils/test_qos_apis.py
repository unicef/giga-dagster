from unittest.mock import MagicMock, patch

import pytest
from src.utils.qos_apis.common import (
    _generate_auth_parameters,
    _make_api_request,
    _update_parameters,
)
from src.utils.qos_apis.school_connectivity import query_school_connectivity_data


@pytest.fixture
def mock_db():
    session = MagicMock()
    return session


def test_generate_auth_parameters():
    row_data = {
        "authorization_type": "BEARER_TOKEN",
        "bearer_auth_bearer_token": "secret",
    }
    assert _generate_auth_parameters(row_data) == {"Authorization": "Bearer secret"}

    row_data = {
        "authorization_type": "API_KEY",
        "api_auth_api_key": "x-api-key",
        "api_auth_api_value": "key",
    }
    assert _generate_auth_parameters(row_data) == {"x-api-key": "key"}

    row_data = {"authorization_type": "NONE"}
    assert _generate_auth_parameters(row_data) is None


def test_update_parameters():
    row = {
        "query_parameters": {},
        "request_body": {},
        "target_query": "QUERY_PARAMETERS",
        "target_body": "BODY",
    }

    _update_parameters(row, {"a": 1}, "target_query")
    assert row["query_parameters"] == {"a": 1}

    _update_parameters(row, {"b": 2}, "target_body")
    assert row["request_body"] == {"b": 2}


@patch("src.utils.qos_apis.common.requests.Session")
def test_make_api_request(mock_session, mock_context):
    session = mock_session.return_value
    row_data = {
        "request_method": "GET",
        "api_endpoint": "http://test.com",
        "query_parameters": {"q": 1},
        "request_body": {},
        "data_key": "data",
    }

    resp = MagicMock()
    resp.json.return_value = {"data": [1, 2, 3]}
    session.get.return_value = resp

    result = _make_api_request(mock_context, session, row_data)

    assert result == [1, 2, 3]
    session.get.assert_called_with("http://test.com", params={"q": 1})


@patch("src.utils.qos_apis.school_connectivity._make_api_request")
def test_query_school_connectivity_data_paginated(
    mock_make_request, mock_context, mock_db
):
    row_data = {
        "id": 1,
        "request_body": {},
        "query_parameters": {},
        "pagination_type": "PAGE_NUMBER",
        "page_starts_with": 1,
        "page_number_key": "page",
        "page_size_key": "size",
        "size": 10,
        "school_list": {"name": "Test School"},
        "authorization_type": "NONE",
        "school_id_key": None,
        "date_key": None,
        "request_method": "GET",
        "api_endpoint": "http://test.com",
        "data_key": None,
        "page_send_query_in": "QUERY_PARAMETERS",
    }

    mock_make_request.side_effect = [[{"id": 1}, {"id": 2}], []]

    data = query_school_connectivity_data(mock_context, mock_db, row_data)

    assert len(data) == 2
    assert mock_db.commit.called


@patch("src.utils.qos_apis.school_connectivity._make_api_request")
def test_query_school_connectivity_data_error(mock_make_request, mock_context, mock_db):
    row_data = {
        "id": 1,
        "request_body": {},
        "query_parameters": {},
        "pagination_type": "NONE",
        "authorization_type": "NONE",
        "school_id_key": None,
        "date_key": None,
        "school_id_send_query_in": "NONE",
        "send_date_in": "NONE",
        "request_method": "GET",
        "api_endpoint": "http://test.com",
        "data_key": None,
        "school_list": {"name": "Test School"},
    }

    mock_make_request.side_effect = Exception("API Error")

    with pytest.raises(Exception, match="API Error"):
        query_school_connectivity_data(mock_context, mock_db, row_data)

    assert mock_db.execute.called
    assert mock_db.commit.called
