import os.path
from io import BytesIO

import pandas as pd


def pandas_loader(data: BytesIO, filepath: str) -> pd.DataFrame:
    _, ext = os.path.splitext(filepath)

    if ext == ".csv":
        return pd.read_csv(data)
    if ext in [".xls", ".xlsx"]:
        return pd.read_excel(data, engine="openpyxl")
    if ext == ".json":
        return pd.read_json(data)
    if ext == ".parquet":
        return pd.read_parquet(data)

    raise Exception(f"Unsupported file type `{ext}`")
