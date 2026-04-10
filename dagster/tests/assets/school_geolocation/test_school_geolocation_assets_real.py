import sys
from unittest.mock import MagicMock, patch

mock_trino = MagicMock()
sys.modules["src.utils.db.trino"] = mock_trino

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
    geolocation_delete_staging,
    geolocation_dq_failed_rows,
    geolocation_dq_passed_rows,
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
        StructField("school_name", StringType()),
        StructField("education_level", StringType()),
        StructField("education_level_govt", StringType()),
    ]
    raw_csv = b"school_id,lat,lon,school_name,education_level_govt\n1,10.0,20.0,My School,Primary"
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
            "school_name": "school_name",
            "education_level_govt": "education_level_govt",
        }
        mock_upload.country = "BRA"
        mock_upload.metadata = {"mode": "Create"}

        with (
            patch("src.assets.school_geolocation.assets.FileUploadConfig") as mock_fuc,
            patch(
                "src.assets.school_geolocation.assets.get_schema_columns",
                return_value=mock_cols,
            ),
            patch(
                "src.assets.school_geolocation.assets.datahub_emit_metadata_with_exception_catcher"
            ),
            patch(
                "src.spark.transform_functions.get_nocodb_table_id_from_name",
                return_value="mock_table_id",
            ),
            patch(
                "src.spark.transform_functions.get_nocodb_table_as_key_value_mapping",
                return_value={
                    "primary": "Primary",
                    "secondary": "Secondary",
                    "tertiary": "Tertiary",
                    "unknown": "Unknown",
                },
            ),
            patch("src.spark.transform_functions.settings.DEPLOY_ENV", "local"),
            patch(
                "src.spark.transform_functions.get_admin_boundaries", return_value=None
            ),
            patch(
                "src.spark.transform_functions.add_disputed_region_column",
                side_effect=lambda df: df.withColumn("disputed_region", f.lit(None)),
            ),
        ):
            mock_fuc.from_orm.return_value = mock_upload
            mock_file_config.metadata["mode"] = "Create"

            result = await geolocation_bronze(
                context=op_context,
                geolocation_raw=raw_csv,
                config=mock_file_config,
                spark=mock_spark,
            )

            assert isinstance(result, Output)
            assert result.value.count() == 1

            row = result.value.collect()[0]
            columns = result.value.columns
            assert "latitude" in columns
            assert "longitude" in columns
            assert "school_id_govt" in columns
            assert "education_level" in columns
            assert "school_id_giga" in columns

            assert row["education_level"] == "Primary"
            # verify UUID format roughly (36 characters)
            assert len(str(row["school_id_giga"])) == 36


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
            10.12345,  # 5 decimal precision
            20.12345,  # 5 decimal precision
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
            11.12,  # Bad 2 decimal precision -> fails precision check
            21.12,  # Bad 2 decimal precision
            "Secondary",
            "LOC02",
            "Admin C",
            "Admin D",
            "sig2",
        ),
        (
            "G03",
            "GIGA03",
            None,  # Missing school name -> might trigger specific check depending on mandatory rules
            12.0,
            22.0,
            "Tertiary",
            "LOC03",
            "Admin E",
            "Admin F",
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

    with (
        patch(
            "src.assets.school_geolocation.assets.get_schema_columns",
            return_value=[StructField("school_id_govt", StringType(), True)],
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
        patch("src.data_quality_checks.geography.settings.DEPLOY_ENV", "local"),
        patch("pyspark.sql.DataFrameWriter.saveAsTable"),
    ):
        mock_delta_table.forName.return_value.alias.return_value.toDF.return_value = (
            spark_session.createDataFrame(
                [], schema=StructType([StructField("school_id_govt", StringType())])
            )
        )
        try:
            # Override metadata to be 'create' mode
            mock_file_config.metadata["mode"] = "Create"
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
    assert "dq_has_critical_error" in df.columns
    assert "dq_results" in df.columns
    assert df.count() > 0

    # Let's inspect some of the specific DQ map items.
    # For example the precision failure on School B (second row) should trigger the precision latitude check
    bad_precision_row = df.filter(f.col("school_id_govt") == "G02").collect()[0]
    dq_map = bad_precision_row["dq_results"]
    assert dq_map["precision-latitude"] == 1
    assert dq_map["precision-longitude"] == 1

    good_precision_row = df.filter(f.col("school_id_govt") == "G01").collect()[0]
    dq_map_good = good_precision_row["dq_results"]
    assert dq_map_good["precision-latitude"] == 0
    assert dq_map_good["precision-longitude"] == 0


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
        mock_instance.return_value = None

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
        assert result.metadata["insert_count"].value == 0


@pytest.mark.asyncio
async def test_geolocation_dq_passed_rows(mock_file_config, spark_session, op_context):
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session
    mock_df = spark_session.createDataFrame(
        [("1", 0), ("2", 1)], ["id", "dq_has_critical_error"]
    )

    with (
        patch(
            "src.assets.school_geolocation.assets.dq_split_passed_rows",
            return_value=mock_df.filter("dq_has_critical_error == 0"),
        ),
        patch(
            "src.assets.school_geolocation.assets.get_schema_columns_datahub",
            return_value=[],
        ),
        patch(
            "src.assets.school_geolocation.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch(
            "src.assets.school_geolocation.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_geolocation.assets.get_table_preview", return_value=""
        ),
    ):
        result = await geolocation_dq_passed_rows(
            context=op_context,
            geolocation_data_quality_results=mock_df,
            config=mock_file_config,
            spark=mock_spark,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_geolocation_dq_failed_rows(mock_file_config, spark_session, op_context):
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session
    mock_df = spark_session.createDataFrame(
        [("1", 0), ("2", 1)], ["id", "dq_has_critical_error"]
    )

    with (
        patch(
            "src.assets.school_geolocation.assets.dq_split_failed_rows",
            return_value=mock_df.filter("dq_has_critical_error == 1"),
        ),
        patch(
            "src.assets.school_geolocation.assets.get_schema_columns_datahub",
            return_value=[],
        ),
        patch(
            "src.assets.school_geolocation.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch(
            "src.assets.school_geolocation.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_geolocation.assets.get_table_preview", return_value=""
        ),
    ):
        result = await geolocation_dq_failed_rows(
            context=op_context,
            geolocation_data_quality_results=mock_df,
            config=mock_file_config,
            spark=mock_spark,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_geolocation_delete_staging(mock_file_config, spark_session, op_context):
    mock_spark = MagicMock()
    mock_spark.spark_session = spark_session
    mock_adls = MagicMock()
    mock_adls.download_json.return_value = ["id1", "id2"]

    with (
        patch("src.assets.school_geolocation.assets.StagingStep") as MockStagingStep,
        patch(
            "src.assets.school_geolocation.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch(
            "src.assets.school_geolocation.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_geolocation.assets.get_table_preview", return_value=""
        ),
    ):
        mock_instance = MockStagingStep.return_value
        mock_instance.return_value = spark_session.createDataFrame(
            [("id1", "delete")], ["school_id_govt", "change_type"]
        )

        result = await geolocation_delete_staging(
            context=op_context,
            adls_file_client=mock_adls,
            spark=mock_spark,
            config=mock_file_config,
        )

        assert isinstance(result, Output)
        assert result.value is None
        assert "delete_row_ids" in result.metadata
