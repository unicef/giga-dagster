import os
from unittest.mock import MagicMock, patch

from src.resources.superset import (
    fetch_saved_query,
    get_access_token,
    get_saved_query,
    refresh_access_token,
    run_query,
)


@patch("requests.post")
@patch.dict(
    os.environ,
    {
        "SUPERSET_URL": "http://superset",
        "SUPERSET_USERNAME": "u",
        "SUPERSET_PASSWORD": "p",
    },
)
def test_get_access_token(mock_post):
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"access_token": "token"}
    res = get_access_token()
    assert res == {"access_token": "token"}

    mock_post.return_value.status_code = 401
    mock_post.return_value.text = "Unauthorized"
    res = get_access_token()
    assert res["error"] is True


@patch("requests.post")
@patch.dict(os.environ, {"SUPERSET_URL": "http://superset"})
def test_refresh_access_token(mock_post):
    mock_post.return_value.status_code = 200
    res = refresh_access_token("refresh_token")
    assert res.status_code == 200


@patch("requests.get")
@patch.dict(os.environ, {"SUPERSET_URL": "http://superset"})
def test_get_saved_query(mock_get):
    mock_get.return_value.status_code = 200
    res = get_saved_query("token")
    assert res.status_code == 200


@patch("src.resources.superset.NocoDB")
@patch.dict(
    os.environ, {"CATALOG_TOKEN": "token", "CATALOG_BASE": "base", "DATABASE_ID": "1"}
)
def test_fetch_saved_query(mock_noco_class):
    mock_noco = mock_noco_class.return_value
    mock_base = mock_noco.get_base.return_value
    mock_table = mock_base.get_table_by_title.return_value

    mock_record = MagicMock()
    mock_record.get_values.return_value = {"col": "val"}
    mock_table.get_records.return_value = [mock_record]

    res = fetch_saved_query()
    assert res == [{"col": "val"}]


@patch("requests.post")
@patch.dict(os.environ, {"SUPERSET_URL": "http://superset", "DATABASE_ID": "1"})
def test_run_query(mock_post):
    mock_post.return_value.status_code = 200
    mock_post.return_value.text = "OK"

    query = {"sql": "SELECT 1", "label": "test"}
    res = run_query(query, "token")
    assert res["status_code"] == 200

    from requests.exceptions import Timeout

    mock_post.side_effect = [Timeout("timeout"), MagicMock(status_code=200, text="OK")]
    res = run_query(query, "token")
    assert res["status_code"] == 200
    assert res["attempts"] == 2
