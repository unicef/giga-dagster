import pandas as pd

from dagster import MetadataValue
from src.sensors.base import FileConfig


def get_output_metadata(config: FileConfig, filepath: str = None):
    metadata = {
        **config.dict(exclude={"metadata"}),
        **config.metadata,
    }
    if filepath is not None:
        metadata["filepath"] = filepath

    return metadata


def get_table_preview(df: pd.DataFrame, count: int = 5):
    return MetadataValue.md(df.head(count).to_markdown(index=False))
