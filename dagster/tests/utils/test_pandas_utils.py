from io import BytesIO
from unittest.mock import patch

import pandas as pd
import pytest
from src.exceptions import UnsupportedFiletypeException
from src.utils.pandas import pandas_loader


def test_pandas_loader_csv():
    csv_data = "name,age\nJohn,30\nJane,25"
    data = BytesIO(csv_data.encode("utf-8"))
    df = pandas_loader(data, "test.csv")
    assert len(df) == 2
    assert list(df.columns) == ["name", "age"]
    assert df["name"].tolist() == ["John", "Jane"]


def test_pandas_loader_csv_with_encoding():
    csv_data = "name,age\nJohn,30"
    data = BytesIO(csv_data.encode("utf-8"))
    df = pandas_loader(data, "test.csv")
    assert len(df) == 1


@patch("pandas.read_excel")
def test_pandas_loader_xlsx(mock_read_excel):
    mock_read_excel.return_value = pd.DataFrame({"col": [1, 2]})
    data = BytesIO(b"fake excel data")
    df = pandas_loader(data, "test.xlsx")
    mock_read_excel.assert_called_once()
    assert len(df) == 2


@patch("pandas.read_excel")
def test_pandas_loader_xls(mock_read_excel):
    mock_read_excel.return_value = pd.DataFrame({"col": [1]})
    data = BytesIO(b"fake excel data")
    pandas_loader(data, "test.xls")
    mock_read_excel.assert_called_once()


@patch("pandas.read_json")
def test_pandas_loader_json(mock_read_json):
    mock_read_json.return_value = pd.DataFrame({"col": [1]})
    data = BytesIO(b'{"col": [1]}')
    pandas_loader(data, "test.json")
    mock_read_json.assert_called_once()


@patch("pandas.read_parquet")
def test_pandas_loader_parquet(mock_read_parquet):
    mock_read_parquet.return_value = pd.DataFrame({"col": [1]})
    data = BytesIO(b"fake parquet data")
    pandas_loader(data, "test.parquet")
    mock_read_parquet.assert_called_once()


def test_pandas_loader_unsupported():
    data = BytesIO(b"data")
    with pytest.raises(UnsupportedFiletypeException):
        pandas_loader(data, "test.txt")


def test_pandas_loader_with_dtype_mapping():
    csv_data = "id,value\n1,100\n2,200"
    data = BytesIO(csv_data.encode("utf-8"))
    dtype_mapping = {"id": str}
    df = pandas_loader(data, "test.csv", dtype_mapping=dtype_mapping)
    assert df["id"].dtype == object
