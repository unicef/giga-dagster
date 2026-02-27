from unittest.mock import MagicMock, patch

import pytest
from src.assets.common.assets import (
    broadcast_master_release_notes,
    manual_review_failed_rows,
    manual_review_passed_rows,
    master,
    reference,
    reset_staging_table,
    silver,
)

from dagster import Output


@pytest.mark.asyncio
async def test_manual_review_passed_rows(mock_file_config, spark_session, op_context):
    context = op_context
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    with (
        patch("src.assets.common.assets.get_schema_columns_datahub"),
        patch(
            "src.assets.common.assets.datahub_emit_metadata_with_exception_catcher"
        ) as mock_emit,
    ):
        result = await manual_review_passed_rows(
            context=context, spark=mock_spark_resource, config=mock_file_config
        )
        assert isinstance(result, Output)
        assert result.value is None
        mock_emit.assert_called()


@pytest.mark.asyncio
async def test_broadcast_master_release_notes(
    mock_file_config, spark_session, op_context
):
    context = op_context
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    params = [(1, "A")]
    columns = ["school_id_govt", "name"]
    master_df = spark_session.createDataFrame(params, columns)
    with (
        patch("src.assets.common.assets.send_master_release_notes") as mock_send,
        patch("src.assets.common.assets.get_rest_emitter") as mock_emitter_ctx,
    ):
        mock_send.return_value = {
            "version": 1,
            "rows": 100,
            "added": 10,
            "modified": 0,
            "deleted": 0,
        }
        mock_emitter = MagicMock()
        mock_emitter_ctx.return_value.__enter__.return_value = mock_emitter
        result = await broadcast_master_release_notes(
            context=context,
            config=mock_file_config,
            spark=mock_spark_resource,
            master=master_df,
        )
        assert isinstance(result, Output)
        version = result.metadata["version"]
        if hasattr(version, "value"):
            version = version.value
        assert version == 1
        mock_emitter.emit.assert_called()


@pytest.mark.asyncio
async def test_master(mock_file_config, spark_session, op_context):
    context = op_context
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    spark_session.catalog.refreshTable = MagicMock()
    params = [(1, "A")]
    columns = ["school_id_govt", "name"]
    silver_df = spark_session.createDataFrame(params, columns)
    with (
        patch("src.assets.common.assets.DeltaTable.forName") as mock_dt,
        patch("src.assets.common.assets.check_table_exists", return_value=False),
        patch("src.assets.common.assets.get_schema_columns") as mock_get_schema,
        patch("src.assets.common.assets.add_missing_columns", return_value=silver_df),
        patch("src.assets.common.assets.transform_types", return_value=silver_df),
        patch("src.assets.common.assets.compute_row_hash", return_value=silver_df),
        patch("src.assets.common.assets.get_schema_columns_datahub"),
        patch("src.assets.common.assets.datahub_emit_metadata_with_exception_catcher"),
        patch("src.assets.common.assets.get_output_metadata", return_value={}),
        patch("src.assets.common.assets.get_table_preview", return_value="preview"),
    ):
        mock_dt.return_value.alias.return_value.toDF.return_value = silver_df
        mock_col = MagicMock()
        mock_col.name = "school_id_govt"
        mock_col.nullable = True
        mock_col.dataType = "string"
        mock_get_schema.return_value = [mock_col]
        result = await master(
            context=context, spark=mock_spark_resource, config=mock_file_config
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_reference(mock_file_config, spark_session, op_context):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    spark_session.catalog.refreshTable = MagicMock()
    params = [(1, "A")]
    columns = ["school_id_govt", "name"]
    silver_df = spark_session.createDataFrame(params, columns)
    with (
        patch("src.assets.common.assets.DeltaTable.forName") as mock_dt,
        patch("src.assets.common.assets.check_table_exists", return_value=False),
        patch("src.assets.common.assets.get_schema_columns") as mock_get_schema,
        patch("src.assets.common.assets.add_missing_columns", return_value=silver_df),
        patch("src.assets.common.assets.transform_types", return_value=silver_df),
        patch("src.assets.common.assets.compute_row_hash", return_value=silver_df),
        patch("src.assets.common.assets.get_schema_columns_datahub"),
        patch("src.assets.common.assets.datahub_emit_metadata_with_exception_catcher"),
        patch("src.assets.common.assets.get_output_metadata", return_value={}),
        patch("src.assets.common.assets.get_table_preview", return_value="preview"),
    ):
        mock_dt.return_value.alias.return_value.toDF.return_value = silver_df
        mock_col = MagicMock()
        mock_col.name = "school_id_govt"
        mock_col.nullable = True
        mock_col.dataType = "string"
        mock_get_schema.return_value = [mock_col]
        result = await reference(
            context=op_context, spark=mock_spark_resource, config=mock_file_config
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_silver(mock_file_config, mock_adls_client, spark_session, op_context):
    context = op_context
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    spark_session.catalog.refreshTable = MagicMock()
    mock_adls_client.download_json.return_value = ["__all__"]
    params = [(1, "A", "insert", 1)]
    columns = ["school_id_giga", "name", "_change_type", "_commit_version"]
    staging_df = spark_session.createDataFrame(params, columns)
    mock_session = MagicMock()
    mock_spark_resource.spark_session = mock_session
    mock_session.read.format.return_value.option.return_value.option.return_value.table.return_value = staging_df
    mock_session.sparkContext.broadcast.return_value.value = ["__all__"]
    mock_session.catalog.refreshTable = MagicMock()
    with (
        patch("src.assets.common.assets.DeltaTable.forName") as _,
        patch("src.assets.common.assets.check_table_exists", return_value=False),
        patch("src.assets.common.assets.get_schema_columns") as mock_get_schema,
        patch(
            "src.assets.common.assets.get_primary_key", return_value="school_id_giga"
        ),
        patch(
            "src.assets.common.assets.manual_review_dedupe_strat",
            return_value=staging_df,
        ),
        patch("src.assets.common.assets.get_schema_columns_datahub"),
        patch("src.assets.common.assets.datahub_emit_metadata_with_exception_catcher"),
        patch("src.assets.common.assets.get_output_metadata", return_value={}),
        patch("src.assets.common.assets.get_table_preview", return_value="preview"),
    ):
        mock_col = MagicMock()
        mock_col.name = "school_id_giga"
        mock_get_schema.return_value = [mock_col]
        result = await silver(
            context=context,
            adls_file_client=mock_adls_client,
            spark=mock_spark_resource,
            config=mock_file_config,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_manual_review_failed_rows(
    mock_file_config, mock_adls_client, spark_session, op_context
):
    context = op_context
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    spark_session.catalog.refreshTable = MagicMock()
    mock_adls_client.download_json.return_value = ["__all__"]
    params = [(1, "A", "insert", 1)]
    columns = ["school_id_giga", "name", "_change_type", "_commit_version"]
    staging_df = spark_session.createDataFrame(params, columns)
    mock_session = MagicMock()
    mock_spark_resource.spark_session = mock_session
    mock_session.read.format.return_value.option.return_value.option.return_value.table.return_value = staging_df
    mock_session.catalog.refreshTable = MagicMock()
    with (
        patch("src.assets.common.assets.check_table_exists", return_value=False),
        patch("src.assets.common.assets.get_schema_columns") as mock_get_schema,
        patch(
            "src.assets.common.assets.get_primary_key", return_value="school_id_giga"
        ),
        patch("src.assets.common.assets.get_schema_columns_datahub"),
        patch("src.assets.common.assets.datahub_emit_metadata_with_exception_catcher"),
        patch("src.assets.common.assets.get_output_metadata", return_value={}),
        patch("src.assets.common.assets.get_table_preview", return_value="preview"),
    ):
        mock_col = MagicMock()
        mock_col.name = "school_id_giga"
        mock_get_schema.return_value = [mock_col]
        result = await manual_review_failed_rows(
            context=context,
            adls_file_client=mock_adls_client,
            spark=mock_spark_resource,
            config=mock_file_config,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 0


@pytest.mark.asyncio
async def test_reset_staging_table(
    mock_file_config, mock_adls_client, spark_session, op_context
):
    context = op_context
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    spark_session.catalog.refreshTable = MagicMock()
    spark_session.sql = MagicMock()
    with (
        patch("src.assets.common.assets.get_db_context") as mock_db_ctx,
        patch("src.assets.common.assets.DeltaTable.forName") as _,
        patch("src.assets.common.assets.create_schema") as _,
        patch("src.assets.common.assets.create_delta_table") as _,
        patch("src.assets.common.assets.get_schema_columns") as _,
    ):
        mock_db = MagicMock()
        mock_db_ctx.return_value.__enter__.return_value = mock_db
        mock_db.scalar.return_value = None
        result = await reset_staging_table(
            context=context,
            spark=mock_spark_resource,
            config=mock_file_config,
            adls_file_client=mock_adls_client,
        )
        assert result is None
        spark_session.sql.assert_called()
