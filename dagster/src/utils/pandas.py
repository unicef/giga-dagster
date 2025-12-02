from io import BytesIO
from pathlib import Path

import chardet
import pandas as pd

from src.exceptions import UnsupportedFiletypeException


def pandas_loader(data: BytesIO, filepath: str, dtype_mapping=None) -> pd.DataFrame:
    if dtype_mapping is None:
        dtype_mapping = {}
    ext = Path(filepath).suffix

    if ext == ".csv":
        logger = get_context_with_fallback_logger(context)

        # Detect encoding
        raw_data = data.read()
        detected = chardet.detect(raw_data)
        encoding = detected['encoding'] or 'utf-8'
        confidence = detected['confidence']

        logger.info(f"Loading CSV file {filepath} with detected encoding: {encoding} (confidence: {confidence:.2f})")

        # Reset pointer and read CSV
        data.seek(0)
        return pd.read_csv(
            data,
            dtype=dtype_mapping,
            encoding=encoding
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
