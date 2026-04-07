from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pyspark.sql import functions as f
from src.assets.adhoc.master_csv_to_gold import (
    adhoc__load_master_csv,
    adhoc__load_reference_csv,
    adhoc__master_data_quality_checks,
    adhoc__master_data_transforms,
    adhoc__master_dq_checks_passed,
    adhoc__publish_master_to_gold,
    adhoc__publish_reference_to_gold,
    adhoc__publish_silver_coverage,
    adhoc__publish_silver_geolocation,
    adhoc__reference_data_quality_checks,
)

from dagster import Output


@pytest.fixture
def mock_spark_resource(spark_session):
    mock = MagicMock()
    mock.spark_session = spark_session
    return mock


@pytest.mark.asyncio
async def test_adhoc__load_master_csv(
    mock_adls_client, mock_file_config, mock_spark_resource, op_context
):
    mock_adls_client.download_raw.return_value = (
        b"school_id,name\n1,School A\n2,School B"
    )
    with patch(
        "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
    ) as mock_emit:
        result = await adhoc__load_master_csv(
            context=op_context,
            adls_file_client=mock_adls_client,
            config=mock_file_config,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)
        assert result.value == b"school_id,name\n1,School A\n2,School B"
        mock_emit.assert_called_once()
        mock_adls_client.download_raw.assert_called_with(mock_file_config.filepath)


@pytest.mark.asyncio
async def test_adhoc__master_data_transforms(
    mock_spark_resource, mock_file_config, spark_session, op_context
):
    raw_content = b"school_id_govt,name\n1,School A\n2,School B"
    mock_spark_resource.spark_session = spark_session
    mock_col_govt = MagicMock()
    mock_col_govt.name = "school_id_govt"
    mock_col_admin1 = MagicMock()
    mock_col_admin1.name = "admin1"
    mock_col_admin2 = MagicMock()
    mock_col_admin2.name = "admin2"
    with (
        patch(
            "src.assets.adhoc.master_csv_to_gold.get_schema_columns",
            return_value=[mock_col_govt, mock_col_admin1, mock_col_admin2],
        ),
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        result = await adhoc__master_data_transforms(
            context=op_context,
            adhoc__load_master_csv=raw_content,
            spark=mock_spark_resource,
            config=mock_file_config,
        )
        assert isinstance(result, Output)
        assert isinstance(result.value, pd.DataFrame)
        assert len(result.value) == 2
        assert "school_id_govt" in result.value.columns


@pytest.mark.asyncio
async def test_adhoc__df_duplicates(mock_file_config, spark_session, op_context):
    data = [
        (1, "School A", 1),
        (2, "School B", 2),
    ]
    columns = ["school_id_govt", "name", "row_num"]
    spark_session.createDataFrame(data, columns)


@pytest.mark.asyncio
async def test_adhoc__master_data_quality_checks(
    mock_file_config, spark_session, op_context
):
    data_with_rownum = [(1, "A", 1), (2, "B", 1)]
    columns_with_rownum = ["school_id_govt", "name", "row_num"]
    df_input = spark_session.createDataFrame(data_with_rownum, columns_with_rownum)
    with (
        patch(
            "src.assets.adhoc.master_csv_to_gold.row_level_checks"
        ) as mock_row_checks,
        patch("src.assets.adhoc.master_csv_to_gold.transform_types") as mock_transform,
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        mock_row_checks.return_value = df_input.drop("row_num")
        mock_transform.return_value = df_input.drop("row_num")
        result = await adhoc__master_data_quality_checks(
            context=op_context,
            adhoc__master_data_transforms=df_input,
            config=mock_file_config,
        )
        assert isinstance(result, Output)
        assert isinstance(result.value, pd.DataFrame)
        assert len(result.value) == 2


@pytest.mark.asyncio
async def test_adhoc__master_dq_checks_passed(
    mock_file_config, mock_spark_resource, spark_session, op_context
):
    mock_spark_resource.spark_session = spark_session
    data = [(1, "A")]
    columns = ["school_id_govt", "name"]
    df = spark_session.createDataFrame(data, columns)
    with (
        patch(
            "src.assets.adhoc.master_csv_to_gold.extract_dq_passed_rows",
            return_value=df,
        ),
        patch("src.assets.adhoc.master_csv_to_gold.get_schema_columns_datahub"),
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        result = await adhoc__master_dq_checks_passed(
            context=op_context,
            adhoc__master_data_quality_checks=df,
            config=mock_file_config,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)
        assert len(result.value) == 1


@pytest.mark.asyncio
async def test_adhoc__publish_master_to_gold(
    mock_file_config, mock_spark_resource, spark_session, op_context
):
    mock_spark_resource.spark_session = spark_session
    data = [(1, "A")]
    columns = ["school_id_govt", "name"]
    df = spark_session.createDataFrame(data, columns)
    with (
        patch("src.assets.adhoc.master_csv_to_gold.transform_types", return_value=df),
        patch("src.assets.adhoc.master_csv_to_gold.compute_row_hash", return_value=df),
        patch(
            "src.assets.adhoc.master_csv_to_gold.check_table_exists", return_value=False
        ),
        patch("src.assets.adhoc.master_csv_to_gold.get_schema_columns_datahub"),
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
        patch("src.assets.adhoc.master_csv_to_gold.emit_lineage_base"),
    ):
        result = await adhoc__publish_master_to_gold(
            context=op_context,
            config=mock_file_config,
            adhoc__master_dq_checks_passed=df,
            spark=mock_spark_resource,
            adhoc__master_dq_checks_summary={},
        )
    output_df = result.value
    assert (
        output_df.schema.simpleString() == "struct<school_id_govt:bigint,name:string>"
    )
    assert output_df.count() == 1
    assert output_df.collect()[0].school_id_govt == 1
    assert output_df.collect()[0].name == "A"


@pytest.mark.asyncio
async def test_adhoc__load_reference_csv(
    mock_adls_client, mock_file_config, mock_spark_resource, op_context
):
    mock_adls_client.download_raw.return_value = (
        b"school_id,name\n1,School A\n2,School B"
    )
    with patch(
        "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
    ):
        result = await adhoc__load_reference_csv(
            context=op_context,
            adls_file_client=mock_adls_client,
            config=mock_file_config,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)
        assert result.value == b"school_id,name\n1,School A\n2,School B"


@pytest.mark.asyncio
async def test_adhoc__reference_data_quality_checks(
    mock_file_config, mock_spark_resource, spark_session, op_context
):
    mock_spark_resource.spark_session = spark_session
    raw_content = b"school_id_govt,name\n1,School A\n2,School B"
    mock_col = MagicMock()
    mock_col.name = "school_id_govt"
    mock_col_type = MagicMock()
    mock_col_type.name = "school_id_govt_type"
    data = [(1, "A")]
    columns = ["school_id_govt", "name"]
    df = spark_session.createDataFrame(data, columns)
    with (
        patch(
            "src.assets.adhoc.master_csv_to_gold.get_schema_columns",
            return_value=[mock_col, mock_col_type],
        ),
        patch("src.assets.adhoc.master_csv_to_gold.row_level_checks", return_value=df),
        patch("src.assets.adhoc.master_csv_to_gold.transform_types", return_value=df),
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        result = await adhoc__reference_data_quality_checks(
            context=op_context,
            spark=mock_spark_resource,
            config=mock_file_config,
            adhoc__load_reference_csv=raw_content,
        )
        assert isinstance(result, Output)
        assert len(result.value) == 1


@pytest.mark.asyncio
async def test_adhoc__publish_silver_geolocation(
    mock_file_config, mock_spark_resource, spark_session, op_context
):
    mock_spark_resource.spark_session = spark_session
    data = [(1, "A")]
    columns = ["school_id_govt", "name"]
    df = spark_session.createDataFrame(data, columns)
    mock_col = MagicMock()
    mock_col.name = "school_id_govt"
    mock_col_type = MagicMock()
    mock_col_type.name = "school_id_govt_type"
    mock_col_edu = MagicMock()
    mock_col_edu.name = "education_level_govt"
    with (
        patch(
            "src.assets.adhoc.master_csv_to_gold.get_schema_columns",
            return_value=[mock_col, mock_col_type, mock_col_edu],
        ),
        patch("src.assets.adhoc.master_csv_to_gold.add_missing_columns") as mock_add_fn,
        patch("src.assets.adhoc.master_csv_to_gold.transform_types", return_value=df),
        patch("src.assets.adhoc.master_csv_to_gold.compute_row_hash", return_value=df),
        patch("src.assets.adhoc.master_csv_to_gold.get_schema_columns_datahub"),
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        df_silver = df.withColumn(
            "school_id_govt_type", df["school_id_govt"]
        ).withColumn("education_level_govt", df["school_id_govt"])
        mock_add_fn.return_value = df_silver
        result = await adhoc__publish_silver_geolocation(
            context=op_context,
            config=mock_file_config,
            spark=mock_spark_resource,
            adhoc__master_dq_checks_passed=df,
            adhoc__reference_dq_checks_passed=spark_session.createDataFrame(
                [], df.schema
            ),
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_adhoc__publish_silver_coverage(
    mock_file_config, mock_spark_resource, spark_session, op_context
):
    mock_spark_resource.spark_session = spark_session
    data = [(1, "A")]
    columns = ["school_id_govt", "name"]
    df = spark_session.createDataFrame(data, columns)
    mock_col = MagicMock()
    mock_col.name = "school_id_govt"
    mock_col_cell = MagicMock()
    mock_col_cell.name = "cellular_coverage_availability"
    mock_col_type = MagicMock()
    mock_col_type.name = "cellular_coverage_type"
    with (
        patch(
            "src.assets.adhoc.master_csv_to_gold.get_schema_columns",
            return_value=[mock_col, mock_col_cell, mock_col_type],
        ),
        patch("src.assets.adhoc.master_csv_to_gold.add_missing_columns") as mock_add_fn,
        patch("src.assets.adhoc.master_csv_to_gold.transform_types", return_value=df),
        patch("src.assets.adhoc.master_csv_to_gold.compute_row_hash", return_value=df),
        patch("src.assets.adhoc.master_csv_to_gold.get_schema_columns_datahub"),
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        df_silver = df.withColumn(
            "cellular_coverage_availability", f.lit("Unknown")
        ).withColumn("cellular_coverage_type", f.lit("Unknown"))
        mock_add_fn.return_value = df_silver
        result = await adhoc__publish_silver_coverage(
            context=op_context,
            config=mock_file_config,
            spark=mock_spark_resource,
            adhoc__master_dq_checks_passed=df,
            adhoc__reference_dq_checks_passed=spark_session.createDataFrame(
                [], df.schema
            ),
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_adhoc__publish_reference_to_gold(
    mock_file_config, mock_spark_resource, spark_session, op_context
):
    mock_spark_resource.spark_session = spark_session
    data = [(1, "A")]
    columns = ["school_id_govt", "name"]
    df = spark_session.createDataFrame(data, columns)
    with (
        patch("src.assets.adhoc.master_csv_to_gold.transform_types", return_value=df),
        patch("src.assets.adhoc.master_csv_to_gold.compute_row_hash", return_value=df),
        patch(
            "src.assets.adhoc.master_csv_to_gold.check_table_exists", return_value=False
        ),
        patch("src.assets.adhoc.master_csv_to_gold.get_schema_columns_datahub"),
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        result = await adhoc__publish_reference_to_gold(
            context=op_context,
            config=mock_file_config,
            spark=mock_spark_resource,
            adhoc__reference_dq_checks_passed=df,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1
