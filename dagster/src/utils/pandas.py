from io import BytesIO
from pathlib import Path

import pandas as pd

from src.exceptions import UnsupportedFiletypeException


def pandas_loader(data: BytesIO, filepath: str) -> pd.DataFrame:
    ext = Path(filepath).suffix

    if ext == ".csv":
        return pd.read_csv(data)
    if ext in [".xls", ".xlsx"]:
        return pd.read_excel(data, engine="openpyxl")
    if ext == ".json":
        return pd.read_json(data)
    if ext == ".parquet":
        return pd.read_parquet(data)

    raise UnsupportedFiletypeException(f"Unsupported file type `{ext}`")
