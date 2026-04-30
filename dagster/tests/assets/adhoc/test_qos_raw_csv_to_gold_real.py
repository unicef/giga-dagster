from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.assets.adhoc.qos_raw_csv_to_gold import (
    adhoc__load_qos_raw_csv,
    adhoc__publish_qos_raw_to_gold,
    adhoc__qos_raw_transforms,
)

from dagster import Output


@pytest.mark.asyncio
async def test_adhoc__load_qos_raw_csv(mock_adls_client, mock_file_config, op_context):
    content = b"header\nvalue"
    mock_adls_client.download_raw.return_value = content
    result = await adhoc__load_qos_raw_csv(
        context=op_context, adls_file_client=mock_adls_client, config=mock_file_config
    )
    assert isinstance(result, Output)
    assert result.value == content


@pytest.mark.asyncio
async def test_adhoc__qos_raw_transforms(mock_file_config, spark_session, op_context):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    content = b"school_id_giga,timestamp,val\n1,2023-01-01,10"
    result = await adhoc__qos_raw_transforms(
        context=op_context,
        spark=mock_spark_resource,
        config=mock_file_config,
        adhoc__load_qos_raw_csv=content,
    )
    assert isinstance(result, Output)
    assert isinstance(result.value, pd.DataFrame)
    assert len(result.value) == 1
    assert "signature" in result.value.columns


@pytest.mark.asyncio
async def test_adhoc__publish_qos_raw_to_gold(
    mock_file_config, spark_session, op_context
):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    params = [(1, "A")]
    columns = ["school_id_giga", "name"]
    df = spark_session.createDataFrame(params, columns)
    with (
        patch("src.assets.adhoc.qos_raw_csv_to_gold.transform_types", return_value=df),
        patch(
            "src.assets.adhoc.qos_raw_csv_to_gold.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.adhoc.qos_raw_csv_to_gold.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await adhoc__publish_qos_raw_to_gold(
            context=op_context,
            adhoc__qos_raw_transforms=df,
            config=mock_file_config,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1
