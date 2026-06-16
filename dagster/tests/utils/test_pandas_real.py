from io import BytesIO
from unittest.mock import patch

import pandas as pd
import pytest
from src.exceptions import UnsupportedFiletypeException
from src.utils.pandas import pandas_loader


def test_pandas_loader_csv():
    data = BytesIO(b"col1,col2\n1,a")
    with patch("src.utils.pandas.chardet.detect") as mock_detect:
        mock_detect.return_value = {"encoding": "utf-8", "confidence": 1.0}
        df = pandas_loader(data, "file.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1


def test_pandas_loader_excel():
    data = BytesIO(b"fake_excel_data")
    with patch("src.utils.pandas.pd.read_excel") as mock_read:
        pandas_loader(data, "file.xlsx")
        mock_read.assert_called_with(data, engine="openpyxl", dtype={})
        pandas_loader(data, "file.xls")
        mock_read.assert_called_with(data, engine="xlrd", dtype={})


def test_pandas_loader_unsupported():
    data = BytesIO()
    with pytest.raises(UnsupportedFiletypeException):
        pandas_loader(data, "file.txt")
