from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from src.assets.adhoc.master_dq_checks import (
    adhoc__standalone_master_data_quality_checks,
)

from dagster import Output


@pytest.fixture(scope="module")
def spark_session():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test_master_dq_real")
        .getOrCreate()
    )
    yield spark


@pytest.mark.asyncio
async def test_adhoc__standalone_master_data_quality_checks(
    mock_file_config, spark_session, op_context
):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    mock_dt = MagicMock()
    df_params = [(1, "A")]
    columns = ["school_id_govt", "name"]
    df = spark_session.createDataFrame(df_params, columns)
    mock_dt.toDF.return_value = df
    mock_history_df = MagicMock()
    mock_row = MagicMock()
    mock_row.version = 1
    mock_history_df.orderBy.return_value.first.return_value = mock_row
    mock_dt.history.return_value = mock_history_df
    mock_cdf_df = MagicMock()
    mock_cdf_df.count.return_value = 1
    mock_session = MagicMock()
    mock_spark_resource.spark_session = mock_session
    mock_session.read.format.return_value.option.return_value.option.return_value.table.return_value = mock_cdf_df
    with (
        patch("delta.DeltaTable.forName", return_value=mock_dt),
        patch("src.assets.adhoc.master_dq_checks.row_level_checks", return_value=df),
        patch(
            "src.assets.adhoc.master_dq_checks.get_change_operation_counts"
        ) as mock_counts,
        patch("src.assets.adhoc.master_dq_checks.get_rest_emitter") as mock_emitter_ctx,
        patch(
            "src.assets.adhoc.master_dq_checks.datahub_emit_metadata_with_exception_catcher"
        ),
        patch("src.assets.adhoc.master_dq_checks.get_schema_columns_datahub"),
        patch("src.assets.adhoc.master_dq_checks.get_output_metadata", return_value={}),
        patch(
            "src.assets.adhoc.master_dq_checks.get_table_preview",
            return_value="preview",
        ),
    ):
        mock_counts.return_value = {"added": 1, "modified": 0, "deleted": 0}
        mock_emitter = MagicMock()
        mock_emitter_ctx.return_value.__enter__.return_value = mock_emitter
        result = await adhoc__standalone_master_data_quality_checks(
            context=op_context, config=mock_file_config, spark=mock_spark_resource
        )
        assert isinstance(result, Output)
        mock_emitter.emit.assert_called()


@pytest.mark.asyncio
async def test_adhoc__standalone_master_data_quality_checks_no_changes(
    mock_file_config, spark_session, op_context
):
    mock_spark_resource = MagicMock()
    mock_session = MagicMock()
    mock_spark_resource.spark_session = mock_session
    mock_dt = MagicMock()
    df_params = [(1, "A")]
    columns = ["school_id_govt", "name"]
    real_df = spark_session.createDataFrame(df_params, columns)
    mock_dt.toDF.return_value = real_df
    mock_row = MagicMock()
    mock_row.version = 1
    mock_dt.history.return_value.orderBy.return_value.first.return_value = mock_row
    mock_cdf_df = MagicMock()
    mock_cdf_df.count.return_value = 0
    mock_session.read.format.return_value.option.return_value.option.return_value.table.return_value = mock_cdf_df
    with (
        patch("delta.DeltaTable.forName", return_value=mock_dt),
        patch(
            "src.assets.adhoc.master_dq_checks.row_level_checks", return_value=real_df
        ),
        patch(
            "src.assets.adhoc.master_dq_checks.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        result = await adhoc__standalone_master_data_quality_checks(
            context=op_context, config=mock_file_config, spark=mock_spark_resource
        )
        assert isinstance(result, Output)
        assert result.value is None
