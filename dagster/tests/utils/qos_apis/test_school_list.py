from unittest.mock import MagicMock, patch

import pytest
from src.utils.qos_apis.school_list import query_school_list_data


@pytest.fixture
def mock_db_session():
    session = MagicMock()
    return session


@pytest.fixture
def mock_school_list_row():
    return {
        "id": 1,
        "name": "Test API",
        "request_body": {},
        "query_parameters": {},
        "pagination_type": "NONE",
        "page_starts_with": 0,
    }


def test_query_school_list_none_pagination(
    mock_context, mock_db_session, mock_school_list_row
):
    with (
        patch("src.utils.qos_apis.school_list._make_api_request") as mock_make_req,
        patch("src.utils.qos_apis.school_list._generate_auth_parameters") as mock_auth,
        patch("src.utils.qos_apis.school_list.update") as _,
    ):
        mock_auth.return_value = {"Authorization": "Bearer token"}
        mock_make_req.return_value = [{"id": 1}]

        res = query_school_list_data(
            mock_context, mock_db_session, mock_school_list_row
        )

        assert len(res) == 1
        assert res[0]["id"] == 1

        assert mock_db_session.execute.called
        assert mock_db_session.commit.called


def test_query_school_list_exception(
    mock_context, mock_db_session, mock_school_list_row
):
    with (
        patch("src.utils.qos_apis.school_list._make_api_request") as mock_make_req,
        patch("src.utils.qos_apis.school_list._generate_auth_parameters") as _,
        patch("src.utils.qos_apis.school_list.update") as _,
    ):
        mock_make_req.side_effect = ValueError("API Error")

        with pytest.raises(ValueError):
            query_school_list_data(mock_context, mock_db_session, mock_school_list_row)

        assert mock_db_session.execute.called
        assert mock_db_session.commit.called


def test_query_school_list_pagination(
    mock_context, mock_db_session, mock_school_list_row
):
    mock_school_list_row["pagination_type"] = "PAGE_NUMBER"

    with (
        patch("src.utils.qos_apis.school_list._make_api_request") as mock_make_req,
        patch("src.utils.qos_apis.school_list._generate_auth_parameters") as _,
        patch(
            "src.utils.qos_apis.school_list._generate_pagination_parameters"
        ) as mock_paginate,
        patch("src.utils.qos_apis.school_list.update") as _,
    ):
        mock_make_req.side_effect = [[{"id": 1}], []]

        res = query_school_list_data(
            mock_context, mock_db_session, mock_school_list_row
        )

        assert len(res) == 1
        assert res[0]["id"] == 1
        assert mock_paginate.call_count >= 1
