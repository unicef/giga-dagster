from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.assets.adhoc.master_csv_to_gold import (
    adhoc__load_master_csv,
    adhoc__load_reference_csv,
    adhoc__master_data_quality_checks,
)


@pytest.mark.asyncio
async def test_adhoc__load_master_csv(mock_adls_client, mock_file_config, op_context):
    spark = MagicMock()
    mock_adls_client.download_raw.return_value = b"data"
    with patch(
        "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
    ):
        result = await adhoc__load_master_csv(
            op_context, mock_adls_client, mock_file_config, spark
        )
    assert result.value == b"data"


@pytest.mark.asyncio
async def test_adhoc__load_reference_csv_success(
    mock_adls_client, mock_file_config, op_context
):
    spark = MagicMock()
    mock_adls_client.download_raw.return_value = b"ref_data"
    with patch(
        "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
    ):
        result = await adhoc__load_reference_csv(
            op_context, mock_adls_client, mock_file_config, spark
        )
    assert result.value == b"ref_data"


@patch("src.assets.adhoc.master_csv_to_gold.row_level_checks")
@patch("src.assets.adhoc.master_csv_to_gold.transform_types")
@pytest.mark.asyncio
async def test_adhoc__master_data_quality_checks(
    mock_transform, mock_checks, spark_session, mock_file_config, op_context
):
    schema = StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("row_num", StringType(), True),
        ]
    )
    data = [("1", "1")]
    schema = StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("row_num", IntegerType(), True),
        ]
    )
    data = [("1", 1)]
    df_in = spark_session.createDataFrame(data, schema)
    mock_checks.return_value = df_in
    mock_transform.return_value = df_in
    with patch(
        "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
    ):
        result = await adhoc__master_data_quality_checks(
            op_context, df_in, mock_file_config
        )
    df_out = result.value
    assert isinstance(df_out, pd.DataFrame)
    assert len(df_out) == 1
    mock_checks.assert_called()
