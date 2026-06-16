from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from src.assets.debug.assets import (
    DropSchemaConfig,
    DropTableConfig,
    ExternalDbQueryConfig,
    GenericEmailRequestConfig,
    debug__drop_schema,
    debug__drop_table,
    debug__send_test_email,
    debug__test_connectivity_merge,
    debug__test_mlab_db_connection,
    debug__test_proco_db_connection,
)


@patch("src.assets.debug.assets.PySparkResource")
@pytest.mark.asyncio
async def test_debug__drop_schema(mock_spark_resource, op_context):
    context = op_context
    spark = MagicMock()
    mock_spark_resource.spark_session = spark
    config = DropSchemaConfig(schema_name="test_schema")
    await debug__drop_schema(context, mock_spark_resource, config)
    spark.sql.assert_called_with("DROP SCHEMA IF EXISTS test_schema CASCADE")


@patch("src.assets.debug.assets.PySparkResource")
@pytest.mark.asyncio
async def test_debug__drop_table(mock_spark_resource, op_context):
    context = op_context
    spark = MagicMock()
    mock_spark_resource.spark_session = spark
    config = DropTableConfig(schema_name="test_schema", table_name="test_table")
    await debug__drop_table(context, mock_spark_resource, config)
    spark.sql.assert_called_with("DROP TABLE IF EXISTS test_schema.test_table")


@pytest.mark.asyncio
async def test_debug__test_mlab_db_connection(op_context):
    context = op_context
    config = ExternalDbQueryConfig(country_code="BR")
    path = "src.internal.connectivity_queries"
    with patch(f"{path}.get_mlab_schools") as mock_get:
        df = pd.DataFrame({"col": [1, 2]})
        mock_get.return_value = df
        result = await debug__test_mlab_db_connection(context, config)
        mock_get.assert_called_with("BR", is_test=True)
        assert result.metadata["mlab_schools"] is not None


def test_debug__test_proco_db_connection(op_context):
    context = op_context
    config = ExternalDbQueryConfig(country_code="BR")
    path = "src.internal.connectivity_queries"
    with (
        patch(f"{path}.get_giga_meter_schools") as mock_giga,
        patch(f"{path}.get_rt_schools") as mock_rt,
    ):
        df = pd.DataFrame({"col": [1]})
        mock_giga.return_value = df
        mock_rt.return_value = df
        result = debug__test_proco_db_connection(context, config)
        assert result.metadata["giga_meter_schools"] is not None
        assert result.metadata["rt_schools"] is not None


@pytest.mark.asyncio
async def test_debug__test_connectivity_merge(op_context):
    context = op_context
    config = ExternalDbQueryConfig(country_code="BR")
    path = "src.internal.connectivity_queries"
    with (
        patch(f"{path}.get_giga_meter_schools") as mock_giga,
        patch(f"{path}.get_rt_schools") as mock_rt,
        patch(f"{path}.get_mlab_schools") as mock_mlab,
    ):
        rt_df = pd.DataFrame(
            {
                "school_id_govt": ["1"],
                "country_code": ["BR"],
                "source": ["source1"],
                "country": ["Brazil"],
                "connectivity_rt_ingestion_timestamp": [1],
                "school_id_giga": ["giga1"],
            }
        )
        giga_df = pd.DataFrame(
            {"school_id_giga": ["giga1"], "source_pcdc": ["source2"]}
        )
        mlab_df = pd.DataFrame({"school_id_govt": ["1"], "country_code": ["BR"]})
        mock_rt.return_value = rt_df
        mock_giga.return_value = giga_df
        mock_mlab.return_value = mlab_df
        result = await debug__test_connectivity_merge(context, config)
        assert result.metadata is not None
        assert "rt_schools_summary" in result.metadata


@pytest.mark.asyncio
async def test_debug__send_test_email(op_context):
    context = op_context
    config = GenericEmailRequestConfig(
        recipients=["test@example.com"],
        subject="Test",
        html_part="<b>Hi</b>",
        text_part="Hi",
    )
    with patch("src.assets.debug.assets.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client
        mock_response = MagicMock()
        mock_response.is_error = False
        mock_client.post.return_value = mock_response
        result = await debug__send_test_email(context, config)
        mock_client.post.assert_called()
        assert result.value is None
