from unittest.mock import patch

import pytest
from src.assets.datahub_assets.datahub_assets import (
    datahub__add_business_glossary,
    datahub__create_domains,
    datahub__create_platform_metadata,
    datahub__create_tags,
    datahub__get_azure_ad_users_groups,
    datahub__list_qos_datasets_to_delete,
    datahub__test_connection,
    datahub__update_policies,
)

from dagster import MetadataValue, Output


@patch("src.assets.datahub_assets.datahub_assets.DatahubRestEmitter")
@patch("src.assets.datahub_assets.datahub_assets.settings")
@pytest.mark.asyncio
async def test_datahub__test_connection(mock_settings, mock_emitter_cls, op_context):
    mock_settings.DATAHUB_METADATA_SERVER_URL = "http://localhost:8080"
    mock_settings.DATAHUB_ACCESS_TOKEN = "token"
    mock_emitter = mock_emitter_cls.return_value
    mock_emitter.get_server_config.return_value = {"config": "value"}
    result = await datahub__test_connection(context=op_context)
    assert isinstance(result, Output)
    assert result.metadata["result"] == MetadataValue.json({"config": "value"})
    mock_emitter_cls.assert_called_once()


@patch("src.assets.datahub_assets.datahub_assets.create_domains")
@pytest.mark.asyncio
async def test_datahub__create_domains(mock_create_domains, op_context):
    mock_create_domains.return_value = ["Domain1", "Domain2"]
    await datahub__create_domains(context=op_context)
    mock_create_domains.assert_called_once()


@patch("src.assets.datahub_assets.datahub_assets.create_tags")
@pytest.mark.asyncio
async def test_datahub__create_tags(mock_create_tags, op_context):
    await datahub__create_tags(context=op_context)
    mock_create_tags.assert_called_once_with(op_context)


@patch("src.assets.datahub_assets.datahub_assets.ingest_azure_ad_to_datahub_pipeline")
@pytest.mark.asyncio
async def test_datahub__get_azure_ad_users_groups(mock_ingest, op_context):
    await datahub__get_azure_ad_users_groups(context=op_context)
    mock_ingest.assert_called_once()


@patch("src.assets.datahub_assets.datahub_assets.update_policies")
@pytest.mark.asyncio
async def test_datahub__update_policies(mock_update_policies, op_context):
    await datahub__update_policies(context=op_context)
    mock_update_policies.assert_called_once_with(op_context)


@patch("src.assets.datahub_assets.datahub_assets.add_platform_metadata")
@pytest.mark.asyncio
async def test_datahub__create_platform_metadata(mock_add_metadata, op_context):
    await datahub__create_platform_metadata(context=op_context)
    mock_add_metadata.assert_called_once()
    assert mock_add_metadata.call_args[1]["platform"] == "deltaLake"


@patch("src.assets.datahub_assets.datahub_assets.add_business_glossary")
@pytest.mark.asyncio
async def test_datahub__add_business_glossary(mock_add_glossary, op_context):
    await datahub__add_business_glossary(context=op_context)
    mock_add_glossary.assert_called_once()


@patch("src.assets.datahub_assets.datahub_assets.list_datasets_by_filter")
@pytest.mark.asyncio
async def test_datahub__list_qos_datasets_to_delete(mock_list, op_context):
    mock_list.return_value = [
        "urn:li:dataset:(urn:li:dataPlatform:deltaLake,dq-results,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:deltaLake,other,PROD)",
    ]
    result = await datahub__list_qos_datasets_to_delete(context=op_context)
    assert isinstance(result, Output)
    assert len(result.value) == 1
    assert "dq-results" in result.value[0]
