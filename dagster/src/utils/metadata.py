from typing import Union

import pandas as pd
from pyspark import sql

from dagster import MetadataValue
from src.utils.op_config import FileConfig


def get_output_metadata(config: FileConfig, filepath: str = None):
    metadata = {
        **config.dict(exclude={"metadata"}),
        **config.metadata,
    }
    if filepath is not None:
        metadata["filepath"] = filepath

    return metadata


def get_table_preview(df: Union[pd.DataFrame, sql.DataFrame], count: int = 5):  # noqa:UP007 bug with union of symbols with the same name
    if isinstance(df, sql.DataFrame):
        df_trunc = df.limit(count).toPandas()
    else:
        df_trunc = df.head(count)

    return MetadataValue.md(df_trunc.to_markdown(index=False))
