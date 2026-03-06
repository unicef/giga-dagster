import json
from unittest.mock import patch

import pytest
from src.assets.school_coverage.assets import coverage_raw


def get_valid_config_dict(config):
    d = json.loads(config.json())
    d["tier"] = "RAW"
    return d


@pytest.mark.asyncio
async def test_coverage_raw_simple_invocation(
    mock_file_config,
    mock_spark_resource,
    mock_adls_client,
    op_context,
):
    with (
        patch(
            "src.assets.school_coverage.assets.get_output_metadata"
        ) as mock_get_metadata,
        patch(
            "src.assets.school_coverage.assets.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        mock_get_metadata.return_value = {}
        mock_adls_client.download_raw.return_value = b"raw_data"

        gen = await coverage_raw(
            context=op_context,
            adls_file_client=mock_adls_client,
            config=mock_file_config,
            spark=mock_spark_resource,
        )

        assert gen is not None
