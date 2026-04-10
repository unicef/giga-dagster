from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from src.assets.school_list.assets import (
    qos_school_list_bronze,
    qos_school_list_data_quality_results,
    qos_school_list_data_quality_results_summary,
    qos_school_list_dq_failed_rows,
    qos_school_list_dq_passed_rows,
    qos_school_list_staging,
)
from src.utils.op_config import FileConfig

from dagster import build_op_context


@pytest.fixture
def spark_session():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


@pytest.fixture
def passed_df(spark_session):
    schema = StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("school_name", StringType(), True),
        ]
    )
    return spark_session.createDataFrame([("GOV01", "School 1")], schema)


@pytest.fixture
def mock_spark():
    return MagicMock()


@pytest.fixture
def mock_adls_client():
    return MagicMock()


@pytest.fixture
def mock_file_config():
    return FileConfig(
        filepath="123_BRA_school-coverage_fb_20230101-120000.csv",
        dataset_type="coverage",
        country_code="BRA",
        metastore_schema="schema",
        tier="raw",
        domain="School",
        table_name="table",
    )


@pytest.mark.asyncio
async def test_qos_school_list_dq_passed_rows(passed_df):
    result = await qos_school_list_dq_passed_rows(passed_df)
    assert result.count() == 1


@pytest.mark.asyncio
async def test_qos_school_list_dq_failed_rows(spark_session):
    schema = StructType(
        [
            StructField("school_id_govt", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame([], schema)
    result = await qos_school_list_dq_failed_rows(df)
    assert result.count() == 0


@pytest.mark.asyncio
async def test_qos_school_list_bronze_no_silver(
    spark_session, passed_df, mock_spark, mock_file_config
):
    with (
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview", return_value="preview"
        ),
        patch("src.assets.school_list.assets.check_table_exists", return_value=False),
        patch(
            "src.assets.school_list.assets.create_bronze_layer_columns",
            side_effect=lambda df, **kwargs: df,
        ),
    ):
        result = await qos_school_list_bronze(
            context=MagicMock(),
            qos_school_list_dq_passed_rows=passed_df,
            spark=mock_spark,
            config=mock_file_config,
        )
        assert result.count() == 1
        assert "school_id_govt" in result.columns


@pytest.mark.asyncio
async def test_qos_school_list_bronze_with_silver(
    spark_session, passed_df, mock_spark, mock_file_config
):
    silver_df = spark_session.createDataFrame(
        [("GOV01", "Silver School")], ["school_id_govt", "school_name_silver"]
    )
    with (
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview", return_value="preview"
        ),
        patch("src.assets.school_list.assets.check_table_exists", return_value=True),
        patch("src.assets.school_list.assets.DeltaTable") as mock_delta,
        patch(
            "src.assets.school_list.assets.create_bronze_layer_columns",
            side_effect=lambda df, **kwargs: df,
        ),
    ):
        mock_delta.forName.return_value.toDF.return_value = silver_df
        result = await qos_school_list_bronze(
            context=MagicMock(),
            qos_school_list_dq_passed_rows=passed_df,
            spark=mock_spark,
            config=mock_file_config,
        )
        assert result.count() == 1
        # Should have joined with silver
        assert "school_name_silver" in result.columns


@pytest.mark.asyncio
async def test_qos_school_list_data_quality_results():
    mock_df = MagicMock()
    result = await qos_school_list_data_quality_results(mock_df)
    assert result == mock_df


@pytest.mark.asyncio
async def test_qos_school_list_data_quality_results_summary():
    mock_df = MagicMock()
    result = await qos_school_list_data_quality_results_summary(mock_df)
    assert result == mock_df


@pytest.mark.asyncio
async def test_qos_school_list_staging_functional(
    spark_session, passed_df, mock_spark, mock_adls_client, mock_file_config
):
    op_context = build_op_context()

    # This matches the Schema table structure expected by get_schema_columns
    schema_df = spark_session.createDataFrame(
        [
            ("school_id_govt", "string", True, True),
            ("school_name", "string", True, False),
            ("signature", "string", True, False),
        ],
        ["name", "data_type", "is_nullable", "primary_key"],
    )

    # Mocking the database and Delta table dependencies
    with (
        patch("src.assets.school_list.assets.get_output_metadata", return_value={}),
        patch(
            "src.assets.school_list.assets.get_table_preview", return_value="preview"
        ),
        patch("src.internal.common_assets.staging.get_db_context") as mock_db,
        patch(
            "src.internal.common_assets.staging.check_table_exists", return_value=True
        ),
        patch("src.internal.common_assets.staging.DeltaTable") as mock_delta,
        patch(
            "src.internal.common_assets.staging.get_primary_key",
            return_value="school_id_govt",
        ),
        patch("src.internal.common_assets.staging.emit_lineage_base"),
        patch("src.internal.common_assets.staging.create_delta_table"),
        patch("src.utils.schema.get_schema_table", return_value=schema_df),
        patch("src.utils.schema.DeltaTable") as mock_delta_schema,
        patch("src.spark.transform_functions.DeltaTable") as mock_delta_spark,
        patch("src.utils.delta.DeltaTable") as mock_delta_utils,
    ):
        # Mock Delta table for silver
        silver_df = spark_session.createDataFrame(
            [("GOV01", "Old Name", "sig-123")],
            ["school_id_govt", "school_name", "signature"],
        )
        mock_delta.forName.return_value.toDF.return_value = silver_df
        mock_delta_schema.forName.return_value.toDF.return_value = silver_df
        mock_delta_spark.forName.return_value.toDF.return_value = silver_df
        mock_delta_utils.forName.return_value.toDF.return_value = silver_df

        # Mock DB session for ApprovalRequest and FileUpload
        mock_session = MagicMock()
        mock_db.return_value.__enter__.return_value = mock_session

        # Mock FileUpload record with necessary attributes for Pydantic validation
        mock_file_upload = MagicMock()
        from datetime import datetime

        mock_file_upload.id = "123"
        mock_file_upload.created = datetime(2023, 1, 1)
        mock_file_upload.uploader_id = "user1"
        mock_file_upload.uploader_email = "test@example.com"
        mock_file_upload.dq_report_path = "path/to/dq"
        mock_file_upload.country = "BRA"
        mock_file_upload.dataset = "school_list"
        mock_file_upload.source = "standard"
        mock_file_upload.original_filename = "file.csv"
        mock_file_upload.upload_path = "upload/path"
        mock_file_upload.column_to_schema_mapping = {
            "school_id_govt": "school_id_govt",
            "school_name": "school_name",
        }
        mock_session.scalar.return_value = mock_file_upload

        result = await qos_school_list_staging(
            context=op_context,
            qos_school_list_dq_passed_rows=passed_df,
            adls_file_client=mock_adls_client,
            spark=mock_spark,
            config=mock_file_config,
        )

        # In this functional test, it should run without crashing and return the staging dataframe
        assert result is not None
        assert result.count() >= 0
