from typing import Union

import pandas as pd
from pyspark import sql

from dagster import MarkdownMetadataValue, MetadataValue
from src.utils.op_config import FileConfig


def get_output_metadata(config: FileConfig, filepath: str = None) -> dict[str, str]:
    metadata = {
        **config.dict(exclude={"metadata", "tier"}),
        **config.metadata,
        "tier": config.tier.name,
    }
    if filepath is not None:
        metadata["filepath"] = filepath

    return metadata


def get_table_preview(
    df: Union[pd.DataFrame, sql.DataFrame],  # noqa:UP007 bug with union of symbols with the same name
    count: int = 5,
) -> MarkdownMetadataValue:
    if isinstance(df, sql.DataFrame):
        df_trunc = df.limit(count).toPandas()
    else:
        df_trunc = df.head(count)

    return MetadataValue.md(df_trunc.to_markdown(index=False))
