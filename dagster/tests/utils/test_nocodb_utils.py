from unittest.mock import MagicMock, patch

import pytest
from src.utils.nocodb.get_nocodb_data import (
    get_nocodb_table_as_key_value_mapping,
    get_nocodb_table_as_pandas_dataframe,
    get_nocodb_table_id_from_name,
    get_nocodb_table_rows,
)


@pytest.fixture(autouse=True)
def patch_settings():
    with patch("src.utils.nocodb.get_nocodb_data.settings") as mock_settings:
        mock_settings.NOCODB_BASE_URL = "http://mock-noco"
        mock_settings.NOCODB_TOKEN = "mock-token"
        mock_settings.NOCODB_NAME_MAPPINGS_TABLE_ID = "mapping-id"
        yield mock_settings


@patch("requests.get")
def test_get_nocodb_table_rows(mock_get):
    resp1 = MagicMock()
    resp1.json.return_value = {
        "list": [{"id": 1}],
        "pageInfo": {"isLastPage": False, "pageSize": 10},
    }
    resp2 = MagicMock()
    resp2.json.return_value = {
        "list": [{"id": 2}],
        "pageInfo": {"isLastPage": True, "pageSize": 10},
    }

    mock_get.side_effect = [resp1, resp2]

    rows = get_nocodb_table_rows("table_1")
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[1]["id"] == 2

    assert mock_get.call_count == 2


@patch("src.utils.nocodb.get_nocodb_data.get_nocodb_table_rows")
def test_get_nocodb_table_as_pandas_dataframe(mock_get_rows):
    data = [
        {"a": 1, "b": 2, "c": 3, "d": 4, "e": None},
        {"a": 1, "b": 2, "c": 3, "d": None, "e": None},
    ]
    mock_get_rows.return_value = data

    df = get_nocodb_table_as_pandas_dataframe("table_1")

    assert len(df) == 1
    assert df.iloc[0]["d"] == 4.0


@patch("src.utils.nocodb.get_nocodb_data.get_nocodb_table_rows")
def test_get_nocodb_table_as_key_value_mapping(mock_get_rows):
    data = [
        {"k": "key1", "v": "val1", "ignored": "x"},
        {"k": "key2", "v": "val2", "ignored": "y"},
        {"k": "key3", "v": None, "ignored": "z"},
    ]
    mock_get_rows.return_value = data

    mapping = get_nocodb_table_as_key_value_mapping(
        "t1", key_column="k", value_column="v"
    )
    assert len(mapping) == 3

    assert mapping["key1"] == "val1"

    with pytest.raises(TypeError):
        get_nocodb_table_as_key_value_mapping("t1", key_column="k")


@patch("src.utils.nocodb.get_nocodb_data.get_nocodb_table_rows")
def test_get_nocodb_table_id_from_name(mock_get_rows):
    mock_get_rows.return_value = [{"table_id": "found_id"}]

    tid = get_nocodb_table_id_from_name("my_table")
    assert tid == "found_id"

    call_args = mock_get_rows.call_args
    assert "where" in call_args.kwargs
    assert "my_table" in call_args.kwargs["where"]

    mock_get_rows.return_value = []
    with pytest.raises(ValueError):
        get_nocodb_table_id_from_name("missing")
