from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pyspark.sql import functions as f
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


# ---------------------------------------------------------------------------
# qos_school_list_raw  –  mocks external DB call, tests result shape
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_qos_school_list_raw(mock_file_config, op_context):
    with (
        patch("src.assets.school_list.assets.get_db_context") as mock_db_cntxt,
        patch("src.assets.school_list.assets.query_school_list_data") as mock_query,
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        mock_db = MagicMock()
        mock_db_cntxt.return_value.__enter__.return_value = mock_db
        mock_query.return_value = [
            {"school_id_govt": "GOV01", "school_name": "School A"},
            {"school_id_govt": "GOV02", "school_name": "School B"},
        ]

        result = await qos_school_list_raw(op_context, mock_file_config)

    assert isinstance(result, Output)
    assert isinstance(result.value, pd.DataFrame)
    assert len(result.value) == 2
    assert "school_id_govt" in result.value.columns


# ---------------------------------------------------------------------------
# qos_school_list_dq_passed_rows  –  dq_split_passed_rows runs for real
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_qos_school_list_dq_passed_rows(
    mock_file_config, spark_session, op_context
):
    dq_df = spark_session.createDataFrame(
        [
            ("GOV01", 0),  # passed
            ("GOV02", 1),  # failed
        ],
        ["school_id_govt", "dq_has_critical_error"],
    )

    with (
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await qos_school_list_dq_passed_rows(
            qos_school_list_data_quality_results=dq_df,
            config=mock_file_config,
        )

    assert isinstance(result, Output)
    df_passed = result.value
    assert len(df_passed) == 1
    assert df_passed.iloc[0]["school_id_govt"] == "GOV01"


# ---------------------------------------------------------------------------
# qos_school_list_dq_failed_rows  –  dq_split_failed_rows runs for real
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_qos_school_list_dq_failed_rows(
    mock_file_config, spark_session, op_context
):
    dq_df = spark_session.createDataFrame(
        [
            ("GOV01", 0),
            ("GOV02", 1),
        ],
        ["school_id_govt", "dq_has_critical_error"],
    )

    with (
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await qos_school_list_dq_failed_rows(
            qos_school_list_data_quality_results=dq_df,
            config=mock_file_config,
        )

    assert isinstance(result, Output)
    df_failed = result.value
    assert len(df_failed) == 1
    assert df_failed.iloc[0]["school_id_govt"] == "GOV02"


# ---------------------------------------------------------------------------
# qos_school_list_data_quality_results_summary  –  aggregate functions run
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# qos_school_list_bronze  –  test Spark transformation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_qos_school_list_bronze(mock_file_config, spark_session, op_context):
    raw_df = spark_session.createDataFrame(
        [("GOV01", "School A")], ["school_id", "name"]
    )

    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    config_dict = mock_file_config.dict()
    import json

    config_dict["database_data"] = json.dumps({"column_to_schema_mapping": {}})
    config_dict["dataset_type"] = "school_list"

    from src.utils.op_config import FileConfig

    mock_file_config = FileConfig(**config_dict)

    with (
        patch(
            "src.assets.school_list.assets.column_mapping_rename",
            return_value=(raw_df, {}),
        ),
        patch("src.assets.school_list.assets.get_schema_columns", return_value=[]),
        patch("src.assets.school_list.assets.check_table_exists", return_value=False),
        patch(
            "src.assets.school_list.assets.create_bronze_layer_columns",
            return_value=raw_df,
        ),
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview", return_value="preview"
        ),
    ):
        result = await qos_school_list_bronze(raw_df, mock_file_config, mock_spark)

    assert isinstance(result, Output)
    assert isinstance(result.value, pd.DataFrame)


# ---------------------------------------------------------------------------
# qos_school_list_data_quality_results  –  mocked row_level_checks
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_qos_school_list_data_quality_results(
    mock_file_config, spark_session, op_context
):
    bronze_df = spark_session.createDataFrame(
        [("GOV01", 0)], ["school_id_govt", "some_col"]
    )

    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with (
        patch("src.assets.school_list.assets.get_schema_columns", return_value=[]),
        patch("src.assets.school_list.assets.check_table_exists", return_value=False),
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview", return_value="preview"
        ),
        patch("src.assets.school_list.assets.row_level_checks") as mock_dq,
    ):
        mock_dq.return_value = bronze_df.withColumn("dq_has_critical_error", f.lit(0))

        result = await qos_school_list_data_quality_results(
            context=op_context,
            config=mock_file_config,
            qos_school_list_bronze=bronze_df,
            spark=mock_spark,
        )

    assert isinstance(result, Output)
    assert "dq_has_critical_error" in result.value.columns


# ---------------------------------------------------------------------------
# qos_school_list_data_quality_results_summary  –  mocked aggregates
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_qos_school_list_data_quality_results_summary(
    mock_file_config, spark_session, op_context
):
    bronze_df = spark_session.createDataFrame([("GOV01",)], ["school_id_govt"])
    dq_df = spark_session.createDataFrame(
        [("GOV01", 0)], ["school_id_govt", "dq_has_critical_error"]
    )

    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with (
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch("src.assets.school_list.assets.aggregate_report_json", return_value={}),
        patch(
            "src.assets.school_list.assets.aggregate_report_spark_df",
            return_value=dq_df,
        ),
    ):
        result = await qos_school_list_data_quality_results_summary(
            qos_school_list_bronze=bronze_df,
            qos_school_list_data_quality_results=dq_df,
            spark=mock_spark,
            config=mock_file_config,
        )

    assert isinstance(result, Output)
    assert isinstance(result.value, dict)


# ---------------------------------------------------------------------------
# qos_school_list_staging – smoke test
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_qos_school_list_staging(
    mock_file_config, spark_session, op_context, mock_adls_client
):
    passed_df = spark_session.createDataFrame([("GOV01",)], ["school_id_govt"])

    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with (
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview", return_value="preview"
        ),
        patch("src.assets.school_list.assets.StagingStep") as mock_staging,
        patch(
            "src.assets.school_list.assets.get_schema_columns_datahub", return_value=[]
        ),
        patch(
            "src.assets.school_list.assets.datahub_emit_metadata_with_exception_catcher",
            return_value=None,
        ),
    ):
        mock_staging.return_value.execute.return_value = (passed_df, pd.DataFrame())

        result = await qos_school_list_staging(
            context=op_context,
            qos_school_list_dq_passed_rows=passed_df,
            adls_file_client=mock_adls_client,
            spark=mock_spark,
            config=mock_file_config,
        )

    assert isinstance(result, Output)
    assert result.value is None
