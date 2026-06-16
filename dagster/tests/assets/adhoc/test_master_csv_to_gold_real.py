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
async def test_adhoc__load_reference_csv(
    mock_adls_client, mock_file_config, mock_spark_resource, op_context
):
    mock_adls_client.download_raw.return_value = (
        b"country_code,name\nBRA,Brazil\nPHL,Philippines"
    )
    with patch(
        "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
    ) as mock_emit:
        result = await adhoc__load_reference_csv(
            context=op_context,
            adls_file_client=mock_adls_client,
            config=mock_file_config,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)
        assert result.value == b"country_code,name\nBRA,Brazil\nPHL,Philippines"
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
    # row_num=1 is required for adhoc master DQ check (it deduplicates first)
    # Providing all columns that column_relation_checks might look at for 'master'
    data = [
        (
            "G01",
            "1",
            "School A",
            "admin1",
            "admin2",
            12.3,
            45.6,
            "Primary",
            1,
            "yes",
            "yes",
            "yes",
            10.0,
            "yes",
            "2g",
            "src",
            "2023-01-01",
            "2023-01-01",
            "yes",
            "Grid",
            1,
        ),
    ]
    columns = [
        "school_id_giga",
        "school_id_govt",
        "school_name",
        "admin1",
        "admin2",
        "latitude",
        "longitude",
        "education_level",
        "dq_is_not_within_country",  # will be overwritten but shouldn't fail
        "connectivity",
        "connectivity_RT",
        "connectivity_govt",
        "download_speed_contracted",
        "cellular_coverage_availability",
        "cellular_coverage_type",
        "connectivity_RT_datasource",
        "connectivity_RT_ingestion_timestamp",
        "connectivity_govt_ingestion_timestamp",
        "electricity_availability",
        "electricity_type",
        "row_num",
    ]
    df_input = spark_session.createDataFrame(data, columns)

    with (
        patch("src.assets.adhoc.master_csv_to_gold.transform_types") as mock_transform,
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        mock_transform.side_effect = lambda df, *args: df
        result = await adhoc__master_data_quality_checks(
            context=op_context,
            adhoc__master_data_transforms=df_input,
            config=mock_file_config,
        )
        assert isinstance(result, Output)
        assert isinstance(result.value, pd.DataFrame)
        assert len(result.value) == 1
        assert "dq_has_critical_error" in result.value.columns
        # verify real DQ logic executed
        assert (
            "dq_column_relation_checks-cellular_coverage_availability_cellular_coverage_type"
            in result.value.columns
        )


@pytest.mark.asyncio
async def test_adhoc__master_dq_checks_passed(
    mock_file_config, mock_spark_resource, spark_session, op_context
):
    mock_spark_resource.spark_session = spark_session
    # Row 1 passed, Row 2 failed
    data = [
        (1, "A", 0),
        (2, "B", 1),
    ]
    columns = ["school_id_govt", "name", "dq_has_critical_error"]
    df = spark_session.createDataFrame(data, columns)
    with (
        patch("src.assets.adhoc.master_csv_to_gold.get_schema_columns_datahub"),
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
        # extract_dq_passed_rows calls get_schema_columns internally if master/reference
        patch("src.data_quality_checks.utils.get_schema_columns") as mock_get_schema,
    ):
        mock_get_schema.return_value = [
            MagicMock(name="school_id_govt"),
            MagicMock(name="name"),
        ]
        mock_get_schema.return_value[0].name = "school_id_govt"
        mock_get_schema.return_value[1].name = "name"

        result = await adhoc__master_dq_checks_passed(
            context=op_context,
            adhoc__master_data_quality_checks=df,
            config=mock_file_config,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)
        assert len(result.value) == 1
        assert result.value.iloc[0]["school_id_govt"] == 1


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
async def test_adhoc__reference_data_quality_checks(
    mock_file_config, mock_spark_resource, spark_session, op_context
):
    mock_spark_resource.spark_session = spark_session
    # school_id_giga and education_level_govt are mandatory for reference
    raw_content = b"school_id_giga,education_level_govt\nG01,Primary\nG02,Secondary"

    mock_cols = [MagicMock(), MagicMock(), MagicMock()]
    mock_cols[0].name = "school_id_giga"
    mock_cols[1].name = "education_level_govt"
    mock_cols[2].name = "school_id_govt_type"

    with (
        patch(
            "src.assets.adhoc.master_csv_to_gold.get_schema_columns",
            return_value=mock_cols,
        ),
        patch("src.assets.adhoc.master_csv_to_gold.transform_types") as mock_transform,
        patch(
            "src.assets.adhoc.master_csv_to_gold.datahub_emit_metadata_with_exception_catcher"
        ),
    ):
        mock_transform.side_effect = lambda df, *args: df
        result = await adhoc__reference_data_quality_checks(
            context=op_context,
            spark=mock_spark_resource,
            config=mock_file_config,
            adhoc__load_reference_csv=raw_content,
        )
        assert isinstance(result, Output)
        assert len(result.value) == 2
        assert "dq_has_critical_error" in result.value.columns
        assert all(result.value["dq_has_critical_error"] == 0)


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
