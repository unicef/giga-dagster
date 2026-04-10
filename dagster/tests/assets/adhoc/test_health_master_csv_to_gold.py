from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pyspark.sql.types import (
    StringType,
    StructField,
)
from src.assets.adhoc.health_master_csv_to_gold import (
    adhoc__health_master_data_transforms,
    adhoc__load_health_master_csv,
)

from dagster import Output


@pytest.mark.asyncio
async def test_adhoc__load_health_master_csv(
    mock_adls_client, mock_file_config, op_context
):
    mock_adls_client.download_raw.return_value = b"raw_content"
    result = adhoc__load_health_master_csv(
        op_context, mock_adls_client, mock_file_config
    )
    assert result.value == b"raw_content"


@pytest.mark.asyncio
async def test_adhoc__health_master_data_transforms_functional(
    spark_session, mock_file_config, op_context
):
    # Setup test data
    raw_csv = b"name,lat,lon\nHealth A,12.3,45.6"

    mock_adls_client = MagicMock()
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    # Mock schema columns
    mock_cols = [
        StructField("name", StringType()),
        StructField("lat", StringType()),
        StructField("lon", StringType()),
        StructField("health_id_giga", StringType()),
    ]

    with (
        patch(
            "src.assets.adhoc.health_master_csv_to_gold.get_schema_columns",
            return_value=mock_cols,
        ),
        patch(
            "src.assets.adhoc.health_master_csv_to_gold.get_output_metadata",
            return_value={},
        ),
        patch(
            "src.assets.adhoc.health_master_csv_to_gold.get_table_preview",
            return_value="preview",
        ),
        patch(
            "src.spark.transform_functions.get_admin_boundaries", return_value=None
        ),  # Mock to return 'Unknown'
    ):
        result = await adhoc__health_master_data_transforms(
            context=op_context,
            adhoc__load_health_master_csv=raw_csv,
            spark=mock_spark,
            adls_file_client=mock_adls_client,
            config=mock_file_config,
        )

    assert isinstance(result, Output)
    df = result.value
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert "health_id_giga" in df.columns
    assert "admin1" in df.columns
    assert df.iloc[0]["admin1"] == "Unknown"
    mock_adls_client.upload_pandas_dataframe_as_file.assert_called()
