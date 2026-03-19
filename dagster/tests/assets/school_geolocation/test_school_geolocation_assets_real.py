import sys
from unittest.mock import MagicMock, patch

mock_trino = MagicMock()
sys.modules["src.utils.db.trino"] = mock_trino

import pandas as pd
import pytest
from pyspark.sql import functions as f
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.assets.school_geolocation.assets import (
    geolocation_bronze,
    geolocation_data_quality_results,
    geolocation_metadata,
    geolocation_raw,
    geolocation_staging,
)
from src.constants.data_tier import DataTier
from src.utils.op_config import FileConfig

from dagster import Output


@pytest.fixture
def mock_file_config():
    return FileConfig(
        filepath="raw/school_geolocation/BRA/123_BRA_school-geolocation_20230101-120000.csv",
        dataset_type="school_geolocation",
        country_code="BRA",
        metastore_schema="school_geolocation",
        tier=DataTier.RAW,
        file_size_bytes=100,
        destination_filepath="raw/school_geolocation/BRA/123_BRA_school-geolocation_20230101-120000.csv",
        metadata={"mode": "append"},
    )


async def test_geolocation_raw(
    mock_file_config, spark_session, mock_adls_client, op_context
):
    mock_adls_client.download_raw = MagicMock(
        return_value=b"school_id,lat,lon\n1,10.0,20.0"
    )
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    assert mock_adls_client.download_raw() == b"school_id,lat,lon\n1,10.0,20.0"

    result = await geolocation_raw(
        context=op_context,
        adls_file_client=mock_adls_client,
        config=mock_file_config,
        spark=mock_spark,
    )

    mock_adls_client.download_raw.assert_called()

    assert result.value == b"school_id,lat,lon\n1,10.0,20.0"


@pytest.mark.asyncio
async def test_geolocation_metadata(mock_file_config, spark_session, op_context):
    raw_bytes = b"header1,header2\nval1,val2"
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with (
        patch(
            "src.assets.school_geolocation.assets.get_schema_columns"
        ) as mock_get_columns,
        patch("src.assets.school_geolocation.assets.DeltaTable") as mock_delta_table,
        patch("src.assets.school_geolocation.assets.create_schema") as _,
        patch("src.assets.school_geolocation.assets.create_delta_table") as _,
        patch.object(spark_session.catalog, "refreshTable"),
    ):
        mock_get_columns.return_value = [
            StructField("col1", StringType()),
            StructField("col2", IntegerType()),
        ]

        mock_dt_instance = MagicMock()
        mock_delta_table.forName.return_value = mock_dt_instance
        mock_dt_instance.alias.return_value.merge.return_value.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.return_value = None

        result = geolocation_metadata(
            context=op_context,
            geolocation_raw=raw_bytes,
            config=mock_file_config,
            spark=mock_spark,
        )

        assert isinstance(result, Output), f"Expected Output, got {type(result)}"
        assert result.value is None


@pytest.mark.asyncio
async def test_geolocation_bronze(mock_file_config, spark_session, op_context):
    """Test geolocation_bronze runs create_bronze_layer_columns for real."""
    mock_cols = [
        StructField("school_id_govt", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
    ]
    raw_csv = b"school_id,lat,lon\n1,10.0,20.0"
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    with patch("src.assets.school_geolocation.assets.get_db_context") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db

        mock_upload = MagicMock()
        mock_upload.column_to_schema_mapping = {
            "school_id": "school_id_govt",
            "lat": "latitude",
            "lon": "longitude",
        }
        mock_upload.country = "BRA"
        mock_upload.metadata = {"mode": "append"}

        with patch("src.assets.school_geolocation.assets.FileUploadConfig") as mock_fuc:
            mock_fuc.from_orm.return_value = mock_upload

            with patch(
                "src.assets.school_geolocation.assets.get_schema_columns",
                return_value=[],
            ):
                with patch(
                    "src.assets.school_geolocation.assets.get_country_rt_schools",
                    return_value=spark_session.createDataFrame([], StructType([])),
                ):
                    with patch(
                        "src.assets.school_geolocation.assets.merge_connectivity_to_df",
                        side_effect=lambda df, *args, **kwargs: df,
                    ):
                        with patch(
                            "src.assets.school_geolocation.assets.standardize_connectivity_type",
                            side_effect=lambda df, *args, **kwargs: df,
                        ):
                            # We want to let column_mapping_rename and create_bronze_layer_columns run for real
                            pass
            with patch(
                "src.assets.school_geolocation.assets.get_schema_columns",
                return_value=mock_cols,
            ):
                with patch(
                    "src.assets.school_geolocation.assets.get_country_rt_schools",
                    return_value=spark_session.createDataFrame([], StructType([])),
                ):
                    with patch(
                        "src.assets.school_geolocation.assets.merge_connectivity_to_df",
                        side_effect=lambda df, *args, **kwargs: df,
                    ):
                        with patch(
                            "src.assets.school_geolocation.assets.standardize_connectivity_type",
                            side_effect=lambda df, *args, **kwargs: df,
                        ):
                            with patch(
                                "src.spark.transform_functions.get_nocodb_table_id_from_name",
                                return_value="123",
                            ):
                                with patch(
                                    "src.spark.transform_functions.get_nocodb_table_as_key_value_mapping",
                                    return_value={},
                                ):
                                    with patch(
                                        "src.assets.school_geolocation.assets.create_bronze_layer_columns",
                                        side_effect=lambda df, *args, **kwargs: df,
                                    ):
                                        with patch(
                                            "src.assets.school_geolocation.assets.datahub_emit_metadata_with_exception_catcher"
                                        ):
                                            result = await geolocation_bronze(
                                                context=op_context,
                                                geolocation_raw=raw_csv,
                                                config=mock_file_config,
                                                spark=mock_spark,
                                            )

                            assert isinstance(result, Output)
                            assert isinstance(result.value, pd.DataFrame)
                            assert len(result.value) == 1
                            assert "latitude" in result.value.columns
                            assert "longitude" in result.value.columns
                            assert "school_id_govt" in result.value.columns


@pytest.mark.asyncio
async def test_geolocation_data_quality_results(
    mock_file_config, spark_session, op_context
):
    """Test geolocation_data_quality_results runs row_level_checks for real."""
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    # Create a bronze-like dataframe with enough columns for geolocation DQ
    bronze_data = [
        (
            "G01",
            "GIGA01",
            "School A",
            10.0,
            20.0,
            "Primary",
            "LOC01",
            "Admin A",
            "Admin B",
            "sig1",
        ),
        (
            "G02",
            "GIGA02",
            "School B",
            11.0,
            21.0,
            "Secondary",
            "LOC02",
            "Admin A",
            "Admin B",
            "sig2",
        ),
        (
            "G03",
            "GIGA03",
            "School C",
            12.0,
            22.0,
            "Tertiary",
            "LOC03",
            "Admin A",
            "Admin B",
            "sig3",
        ),
    ]

    bronze_cols = [
        "school_id_govt",
        "school_id_giga",
        "school_name",
        "latitude",
        "longitude",
        "education_level",
        "location_id",
        "admin1",
        "admin2",
        "signature",
    ]
    bronze_df = spark_session.createDataFrame(bronze_data, bronze_cols)

    def row_level_checks_mock(*args, **kwargs):
        df = args[0] if args else kwargs.get("df")
        df_dq = df.withColumn("dq_has_critical_error", f.lit(0)).withColumn(
            "failure_reason", f.lit("")
        )
        return df_dq

    with (
        patch(
            "src.assets.school_geolocation.assets.get_schema_columns",
            return_value=[StructField("school_id_govt", StringType())],
        ),
        patch(
            "src.assets.school_geolocation.assets.check_table_exists",
            return_value=False,
        ),
        patch("src.assets.school_geolocation.assets.create_schema"),
        patch("src.assets.school_geolocation.assets.create_delta_table"),
        patch(
            "src.assets.school_geolocation.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_geolocation.assets.get_table_preview",
            return_value="preview",
        ),
        patch("src.assets.school_geolocation.assets.DeltaTable") as mock_delta_table,
        patch(
            "src.assets.school_geolocation.assets.row_level_checks",
            side_effect=row_level_checks_mock,
        ),
        patch("pyspark.sql.DataFrameWriter.saveAsTable"),
    ):
        mock_delta_table.forName.return_value.alias.return_value.toDF.return_value = (
            spark_session.createDataFrame(
                [], schema=StructType([StructField("school_id_govt", StringType())])
            )
        )
        try:
            result = await geolocation_data_quality_results(
                context=op_context,
                config=mock_file_config,
                geolocation_bronze=bronze_df,
                spark=mock_spark,
            )
        except Exception as e:
            raise e

    assert isinstance(result, Output)
    df = result.value
    assert isinstance(df, pd.DataFrame)
    assert "dq_has_critical_error" in df.columns
    assert len(df) > 0


@pytest.mark.asyncio
async def test_geolocation_staging(mock_file_config, spark_session, op_context):
    mock_passed_df = spark_session.createDataFrame(
        [("1", "pass")], ["school_id_govt", "dq_status"]
    )
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session

    mock_adls = MagicMock()

    with (
        patch("src.assets.school_geolocation.assets.StagingStep") as MockStagingStep,
        patch(
            "src.assets.school_geolocation.assets.get_schema_columns_datahub"
        ) as mock_get_schema,
        patch(
            "src.assets.school_geolocation.assets.datahub_emit_metadata_with_exception_catcher"
        ) as _,
        patch("src.assets.school_geolocation.assets.get_table_preview") as mock_preview,
    ):
        mock_instance = MockStagingStep.return_value

        mock_staging_result = MagicMock()
        mock_staging_result.count.return_value = 1
        mock_instance.return_value = mock_staging_result

        mock_get_schema.return_value = []
        mock_preview.return_value = "markdown_preview"

        result = await geolocation_staging(
            context=op_context,
            geolocation_dq_passed_rows=mock_passed_df,
            adls_file_client=mock_adls,
            spark=mock_spark,
            config=mock_file_config,
        )

        assert isinstance(result, Output)
        assert result.value is None
        assert result.metadata["row_count"].value == 1
