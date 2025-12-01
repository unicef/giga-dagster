from io import BytesIO
from pathlib import Path

import pandas as pd

from src.exceptions import UnsupportedFiletypeException


def pandas_loader(data: BytesIO, filepath: str, dtype_mapping=None) -> pd.DataFrame:
    if dtype_mapping is None:
        dtype_mapping = {}
    ext = Path(filepath).suffix

    if ext == ".csv":
        return pd.read_csv(
            data,
            dtype=dtype_mapping,
            encoding='utf-8',
            encoding_errors='replace'
        )
    if ext == ".xlsx":
        return pd.read_excel(data, engine="openpyxl", dtype=dtype_mapping)
    if ext == ".xls":
        return pd.read_excel(data, engine="xlrd", dtype=dtype_mapping)
    if ext == ".json":
        return pd.read_json(data, dtype=dtype_mapping)
    if ext == ".parquet":
        return pd.read_parquet(data, dtype=dtype_mapping)

    raise UnsupportedFiletypeException(f"Unsupported file type `{ext}`")
