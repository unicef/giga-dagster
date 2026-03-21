import json
from unittest.mock import MagicMock, patch

import pytest
from src.assets.school_coverage.assets import (
    coverage_bronze,
    coverage_data_quality_results,
    coverage_dq_failed_rows,
    coverage_dq_passed_rows,
    coverage_raw,
)

from dagster import Output


def get_valid_config_dict(config):
    d = json.loads(config.json())
    d["tier"] = "RAW"
    return d


# ---------------------------------------------------------------------------
# coverage_raw
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# coverage_data_quality_results  –  row_level_checks runs for real
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_coverage_data_quality_results(
    mock_file_config, spark_session, op_context
):
    """Feed a CSV with valid + invalid rows and let row_level_checks actually
    run.  We mock only external infra (DB, Delta writes, schema lookup)."""

    # CSV where percent columns sum to 100 for row 1, NOT for row 2
    raw_csv = (
        b"id_input,coverage_input,p2,p3,p4\n"
        b"school-a,yes,30,30,40\n"
        b"school-b,no,10,20,30\n"
    )

    class MockFileUploadConfig:
        column_to_schema_mapping = {
            "id_input": "school_id_giga",
            "coverage_input": "cellular_coverage_availability",
            "p2": "percent_2G",
            "p3": "percent_3G",
            "p4": "percent_4G",
        }

    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with (
        patch("src.assets.school_coverage.assets.get_db_context") as mock_db,
        patch("src.assets.school_coverage.assets.select"),
        patch(
            "src.assets.school_coverage.assets.FileUploadConfig.from_orm",
            return_value=MockFileUploadConfig(),
        ),
        # Mock infra that hits Delta metastore
        patch("src.assets.school_coverage.assets.get_schema_columns", return_value=[]),
        patch(
            "src.assets.school_coverage.assets.add_missing_columns",
            side_effect=lambda df, cols: df,
        ),
        patch("src.assets.school_coverage.assets.create_schema"),
        patch("src.assets.school_coverage.assets.create_delta_table"),
        patch(
            "src.assets.school_coverage.assets.construct_full_table_name",
            return_value="fake_table",
        ),
        patch(
            "src.assets.school_coverage.assets.convert_dq_checks_to_human_readeable_descriptions_and_upload"
        ),
        patch("src.assets.school_coverage.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_coverage.assets.get_table_preview",
            return_value="preview",
        ),
        patch("pyspark.sql.DataFrameWriter.saveAsTable"),
    ):
        # db context manager returns a mock session
        mock_db.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        result = await coverage_data_quality_results(
            context=op_context,
            config=mock_file_config,
            coverage_raw=raw_csv,
            spark=mock_spark,
        )

    assert isinstance(result, Output)
    df = result.value
    assert not df.empty, "DQ results should not be empty"

    # row_level_checks ran for real and added DQ columns
    dq_cols = [c for c in df.columns if c.startswith("dq_")]
    assert len(dq_cols) > 0, "Expected dq_ columns from row_level_checks"
    assert "dq_has_critical_error" in df.columns


# ---------------------------------------------------------------------------
# coverage_dq_passed_rows  –  uses dq_split_passed_rows for real
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_coverage_dq_passed_rows(mock_file_config, spark_session, op_context):
    """Feed a DQ-results DataFrame and let dq_split_passed_rows filter it."""

    dq_data = [
        ("school-a", 0),  # passed
        ("school-b", 1),  # failed
    ]
    dq_df = spark_session.createDataFrame(
        dq_data, ["school_id_giga", "dq_has_critical_error"]
    )

    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with (
        patch(
            "src.assets.school_coverage.assets.get_schema_columns_datahub",
            return_value=[],
        ),
        patch(
            "src.assets.school_coverage.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch("src.assets.school_coverage.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_coverage.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await coverage_dq_passed_rows(
            context=op_context,
            coverage_data_quality_results=dq_df,
            config=mock_file_config,
            spark=mock_spark,
        )

    assert isinstance(result, Output)
    df_passed = result.value
    # Only the row with dq_has_critical_error == 0 should pass
    assert len(df_passed) == 1
    assert df_passed.iloc[0]["school_id_giga"] == "school-a"


# ---------------------------------------------------------------------------
# coverage_dq_failed_rows  –  uses dq_split_failed_rows for real
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_coverage_dq_failed_rows(mock_file_config, spark_session, op_context):
    """Feed a DQ-results DataFrame and let dq_split_failed_rows filter it."""

    dq_data = [
        ("school-a", 0),
        ("school-b", 1),
    ]
    dq_df = spark_session.createDataFrame(
        dq_data, ["school_id_giga", "dq_has_critical_error"]
    )

    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with (
        patch(
            "src.assets.school_coverage.assets.get_schema_columns_datahub",
            return_value=[],
        ),
        patch(
            "src.assets.school_coverage.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch("src.assets.school_coverage.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_coverage.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await coverage_dq_failed_rows(
            context=op_context,
            coverage_data_quality_results=dq_df,
            config=mock_file_config,
            spark=mock_spark,
        )

    assert isinstance(result, Output)
    df_failed = result.value
    assert len(df_failed) == 1
    assert df_failed.iloc[0]["school_id_giga"] == "school-b"


# ---------------------------------------------------------------------------
# coverage_bronze  –  fb path (source="fb")
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_coverage_bronze_fb(mock_file_config, spark_session, op_context):
    """Test coverage_bronze passes data through fb_transforms for fb source.
    fb_transforms internally calls get_schema_columns (Delta infra) so we mock it,
    but we let the rest of the coverage_bronze logic run for real."""

    passed_data = [("G01", "yes", "4G")]
    passed_df = spark_session.createDataFrame(
        passed_data,
        ["school_id_giga", "cellular_coverage_availability", "cellular_coverage_type"],
    )

    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with (
        # fb_transforms calls get_schema_columns internally, so mock the whole transform
        patch(
            "src.assets.school_coverage.assets.fb_transforms",
            return_value=passed_df,
        ),
        patch(
            "src.assets.school_coverage.assets.get_schema_columns_datahub",
            return_value=[],
        ),
        patch(
            "src.assets.school_coverage.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch("src.assets.school_coverage.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_coverage.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        spark_session.catalog.tableExists = MagicMock(return_value=False)
        result = await coverage_bronze(
            context=op_context,
            coverage_dq_passed_rows=passed_df,
            spark=mock_spark,
            config=mock_file_config,
        )

    assert isinstance(result, Output)
    df = result.value
    assert not df.empty
    assert "school_id_giga" in df.columns
    assert len(df) == 1
