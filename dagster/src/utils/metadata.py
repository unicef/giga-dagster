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
    df,  # Can be either pandas.DataFrame or pyspark.sql.DataFrame
    count: int = 5,
) -> MarkdownMetadataValue:
    """Generate a markdown preview of the first few rows of a DataFrame.

    Args:
        df: Either a Pandas DataFrame or Spark DataFrame to preview
        count: Number of rows to show in the preview

    Returns:
        A markdown formatted preview of the DataFrame
    """
    # Handle empty DataFrame
    if (isinstance(df, sql.DataFrame) and not df.head()) or (
        hasattr(df, "empty") and df.empty
    ):
        return MetadataValue.md("*Empty DataFrame*")

    # Get the first n rows
    if isinstance(df, sql.DataFrame):
        preview_rows = df.limit(count).collect()
        columns = df.columns
    else:
        preview_rows = df.head(count).to_dict(orient="records")
        columns = df.columns.tolist()

    # Build markdown table
    header = "| " + " | ".join(columns) + " |"
    separator = "|" + "|".join(["---"] * len(columns)) + "|"

    # Convert rows to markdown
    rows = []
    for row in preview_rows:
        if isinstance(df, sql.DataFrame):
            row_values = [str(row[col] or "") for col in columns]
        else:
            row_values = [str(row[col] or "") for col in columns]
        rows.append("| " + " | ".join(row_values) + " |")

    markdown = "\n".join([header, separator] + rows)
    return MetadataValue.md(markdown)
