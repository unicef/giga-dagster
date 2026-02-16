import sys
from unittest.mock import MagicMock, patch

mock_trino = MagicMock()
sys.modules["src.utils.db.trino"] = mock_trino

import pandas as pd
import pytest
from src.assets.school_geolocation.assets import (
    geolocation_bronze,
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


@pytest.mark.asyncio
async def test_geolocation_raw(
    mock_file_config, spark_session, mock_adls_client, op_context
):
    context = op_context

    mock_adls_client.download_raw = MagicMock(
        return_value=b"school_id,lat,lon\n1,10.0,20.0"
    )

    assert mock_adls_client.download_raw() == b"school_id,lat,lon\n1,10.0,20.0"

    result = await geolocation_raw(
        context=context,
        adls_file_client=mock_adls_client,
        config=mock_file_config,
        spark=MagicMock(),
    )

    mock_adls_client.download_raw.assert_called()

    assert result.value == b"school_id,lat,lon\n1,10.0,20.0"


from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField


@pytest.mark.asyncio
async def test_geolocation_metadata(mock_file_config, spark_session, op_context):
    context = op_context
    raw_bytes = b"header1,header2\nval1,val2"

    with (
        patch(
            "src.assets.school_geolocation.assets.get_schema_columns"
        ) as mock_get_columns,
        patch("src.assets.school_geolocation.assets.DeltaTable") as mock_delta_table,
        patch("src.assets.school_geolocation.assets.create_schema") as _,
        patch("src.assets.school_geolocation.assets.create_delta_table") as _,
    ):
        mock_get_columns.return_value = [
            StructField("col1", StringType()),
            StructField("col2", IntegerType()),
        ]

        mock_dt_instance = MagicMock()
        mock_delta_table.forName.return_value = mock_dt_instance
        mock_dt_instance.alias.return_value.merge.return_value.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.return_value = None

        result = geolocation_metadata(
            context=context,
            geolocation_raw=raw_bytes,
            config=mock_file_config,
            spark=MagicMock(),
        )

        assert isinstance(result, Output)
        assert result.value is None


@pytest.mark.asyncio
async def test_geolocation_bronze(mock_file_config, spark_session, op_context):
    context = op_context
    raw_csv = b"school_id_govt,lat,lon\n1,10.0,20.0"

    with patch("src.assets.school_geolocation.assets.get_db_context") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db

        mock_upload = MagicMock()
        mock_upload.column_to_schema_mapping = {
            "school_id_govt": "school_id_govt",
            "lat": "lat",
            "lon": "lon",
        }
        mock_upload.country = "BRA"
        mock_upload.metadata = {"mode": "append"}

        with patch("src.assets.school_geolocation.assets.FileUploadConfig") as mock_fuc:
            mock_fuc.from_orm.return_value = mock_upload

            with patch(
                "src.assets.school_geolocation.assets.get_schema_columns"
            ) as mock_cols:
                mock_cols.return_value = [
                    StructField("school_id_govt", StringType()),
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType()),
                ]

                with patch(
                    "src.assets.school_geolocation.assets.create_bronze_layer_columns"
                ) as mock_create:
                    mock_df = MagicMock()
                    mock_df.toPandas.return_value = pd.DataFrame(
                        [{"school_id_govt": "1"}]
                    )
                    mock_df.columns = ["school_id_govt"]
                    mock_create.return_value = mock_df

                    with patch(
                        "src.assets.school_geolocation.assets.get_country_rt_schools"
                    ) as mock_rt:
                        mock_rt.return_value = MagicMock()

                        with patch(
                            "src.assets.school_geolocation.assets.merge_connectivity_to_df"
                        ) as mock_merge:
                            mock_merge.return_value = mock_df

                            with patch(
                                "src.assets.school_geolocation.assets.standardize_connectivity_type"
                            ) as mock_std:
                                mock_std.return_value = mock_df

                                result = await geolocation_bronze(
                                    context=context,
                                    geolocation_raw=raw_csv,
                                    config=mock_file_config,
                                    spark=MagicMock(),
                                )

                                assert isinstance(result, Output)
                                assert isinstance(result.value, pd.DataFrame)
                                assert len(result.value) == 1


@pytest.mark.asyncio
async def test_geolocation_staging(mock_file_config, spark_session, op_context):
    context = op_context

    mock_passed_df = spark_session.createDataFrame(
        [("1", "pass")], ["school_id_govt", "dq_status"]
    )

    mock_adls = MagicMock()
    mock_spark = MagicMock()

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
        mock_spark.spark_session = spark_session

        result = await geolocation_staging(
            context=context,
            geolocation_dq_passed_rows=mock_passed_df,
            adls_file_client=mock_adls,
            spark=mock_spark,
            config=mock_file_config,
        )

        assert isinstance(result, Output)
        assert result.value is None
        assert result.metadata["row_count"].value == 1
