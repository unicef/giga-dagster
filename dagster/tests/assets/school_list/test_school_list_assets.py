from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.assets.school_list.assets import (
    qos_school_list_bronze,
    qos_school_list_data_quality_results,
    qos_school_list_data_quality_results_summary,
    qos_school_list_dq_failed_rows,
    qos_school_list_dq_passed_rows,
    qos_school_list_raw,
    qos_school_list_staging,
)

from dagster import Output


@pytest.mark.asyncio
async def test_qos_school_list_raw(
    mock_file_config,
    op_context,
):
    with (
        patch("src.assets.school_list.assets.get_db_context") as mock_db_cntxt,
        patch("src.assets.school_list.assets.query_school_list_data") as mock_query,
        patch("src.assets.school_list.assets.get_output_metadata") as mock_get_metadata,
        patch("src.assets.school_list.assets.get_table_preview") as mock_preview,
    ):
        mock_db = MagicMock()
        mock_db_cntxt.return_value.__enter__.return_value = mock_db
        mock_query.return_value = [{"col1": 1, "col2": "a"}]
        mock_get_metadata.return_value = {"meta": "data"}
        mock_preview.return_value = "preview"

        result = await qos_school_list_raw(op_context, mock_file_config)

        assert isinstance(result, Output)
        assert isinstance(result.value, pd.DataFrame)
        assert len(result.value) == 1


@pytest.mark.asyncio
async def test_qos_school_list_bronze_smoke(
    mock_file_config,
):
    assert qos_school_list_bronze is not None
    assert callable(qos_school_list_bronze)


@pytest.mark.asyncio
async def test_qos_school_list_data_quality_results_smoke():
    assert qos_school_list_data_quality_results is not None
    assert callable(qos_school_list_data_quality_results)


@pytest.mark.asyncio
async def test_qos_school_list_data_quality_results_summary_smoke():
    assert qos_school_list_data_quality_results_summary is not None
    assert callable(qos_school_list_data_quality_results_summary)


@pytest.mark.asyncio
async def test_qos_school_list_dq_passed_rows_smoke():
    assert qos_school_list_dq_passed_rows is not None
    assert callable(qos_school_list_dq_passed_rows)


@pytest.mark.asyncio
async def test_qos_school_list_dq_failed_rows_smoke():
    assert qos_school_list_dq_failed_rows is not None
    assert callable(qos_school_list_dq_failed_rows)


@pytest.mark.asyncio
async def test_qos_school_list_staging_smoke():
    assert qos_school_list_staging is not None
    assert callable(qos_school_list_staging)
