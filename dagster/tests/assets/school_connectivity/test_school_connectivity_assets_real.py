import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from src.assets.school_connectivity.assets import (
    connectivity_broadcast_master_release_notes,
    qos_school_connectivity_bronze,
    qos_school_connectivity_data_quality_results,
    qos_school_connectivity_data_quality_results_summary,
    qos_school_connectivity_dq_failed_rows,
    qos_school_connectivity_dq_passed_rows,
    qos_school_connectivity_gold,
    qos_school_connectivity_raw,
    school_connectivity_realtime_master,
    school_connectivity_realtime_schools,
    school_connectivity_realtime_silver,
)
from src.constants import DataTier
from src.utils.op_config import FileConfig

from dagster import Output


@pytest.fixture
def mock_file_config():
    row_data = {
        "school_id_key": "school_id",
        "school_list": {
            "school_id_key": "id",
            "column_to_schema_mapping": {"id": "school_id_giga"},
        },
        "school_id_send_query_in": "BODY",
        "has_school_id_giga": True,
        "school_id_giga_govt_key": "school_id",
        "response_date_format": "ISO8601",
        "response_date_key": "timestamp",
    }
    return FileConfig(
        filepath="raw/school_connectivity/BRA/file.csv",
        dataset_type="school_connectivity",
        country_code="BRA",
        file_size_bytes=100,
        destination_filepath="raw/school_connectivity/BRA/file.csv",
        metastore_schema="school_connectivity",
        tier=DataTier.RAW,
        database_data=json.dumps(row_data),
    )


@pytest.mark.asyncio
async def test_qos_school_connectivity_raw(mock_file_config, spark_session, op_context):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    mock_silver_df = spark_session.createDataFrame([("1",)], ["school_id_giga"])
    with (
        patch("src.assets.school_connectivity.assets.get_db_context"),
        patch(
            "src.assets.school_connectivity.assets.query_school_connectivity_data"
        ) as mock_query,
        patch("src.assets.school_connectivity.assets.DeltaTable") as mock_dt_class,
        patch.object(spark_session.catalog, "tableExists", return_value=True),
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        mock_dt_instance = MagicMock()
        mock_dt_class.forName.return_value = mock_dt_instance
        mock_dt_instance.toDF.return_value = mock_silver_df
        mock_query.return_value = [{"school_id": "1", "connectivity": "yes"}]
        result = await qos_school_connectivity_raw(
            context=op_context, config=mock_file_config, spark=mock_spark_resource
        )
        assert isinstance(result, Output)
        assert not result.value.empty
        assert len(result.value) == 1


@pytest.mark.asyncio
async def test_qos_school_connectivity_bronze(
    mock_file_config, spark_session, op_context
):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    raw_df = spark_session.createDataFrame(
        [("1", "2023-01-01T00:00:00")], ["school_id", "timestamp"]
    )
    mock_silver_df = spark_session.createDataFrame([("1",)], ["school_id_giga"])
    with (
        patch("src.assets.school_connectivity.assets.DeltaTable") as mock_dt_class,
        patch("pyspark.sql.catalog.Catalog.tableExists", return_value=True),
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        mock_dt_instance = MagicMock()
        mock_dt_class.forName.return_value = mock_dt_instance
        mock_dt_instance.toDF.return_value = mock_silver_df
        result = await qos_school_connectivity_bronze(
            context=op_context,
            qos_school_connectivity_raw=raw_df,
            config=mock_file_config,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)
        assert not result.value.empty
        assert "signature" in result.value.columns


@pytest.mark.asyncio
async def test_qos_school_connectivity_data_quality_results(
    mock_file_config, spark_session, op_context
):
    # Row 1 is valid, Row 2 has null school_id_giga (critical error)
    bronze_df = spark_session.createDataFrame(
        [("1", "2023-01-01"), (None, "2023-01-02")], ["school_id_giga", "timestamp"]
    )
    with (
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await qos_school_connectivity_data_quality_results(
            context=op_context,
            config=mock_file_config,
            qos_school_connectivity_bronze=bronze_df,
        )
        assert isinstance(result, Output)
        df = result.value
        assert not df.empty
        assert "dq_has_critical_error" in df.columns
        # verify row 1 passed, row 2 failed (school_id_giga is mandatory)
        assert df[df["timestamp"] == "2023-01-01"]["dq_has_critical_error"].iloc[0] == 0
        assert df[df["timestamp"] == "2023-01-02"]["dq_has_critical_error"].iloc[0] == 1


@pytest.mark.asyncio
async def test_qos_school_connectivity_dq_passed_rows(mock_file_config, spark_session):
    dq_results_df = spark_session.createDataFrame(
        [("1", 0), ("2", 1)], ["school_id_giga", "dq_has_critical_error"]
    )
    with (
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await qos_school_connectivity_dq_passed_rows(
            qos_school_connectivity_data_quality_results=dq_results_df,
            config=mock_file_config,
        )
        assert isinstance(result, Output)
        assert len(result.value) == 1
        assert result.value.iloc[0]["school_id_giga"] == "1"


@pytest.mark.asyncio
async def test_qos_school_connectivity_dq_failed_rows(mock_file_config, spark_session):
    dq_results_df = spark_session.createDataFrame(
        [("1", 0), ("2", 1)], ["school_id_giga", "dq_has_critical_error"]
    )
    with (
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await qos_school_connectivity_dq_failed_rows(
            qos_school_connectivity_data_quality_results=dq_results_df,
            config=mock_file_config,
        )
        assert isinstance(result, Output)
        assert len(result.value) == 1
        assert result.value.iloc[0]["school_id_giga"] == "2"


@pytest.mark.asyncio
async def test_qos_school_connectivity_gold(
    mock_file_config, spark_session, op_context
):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    passed_df = spark_session.createDataFrame(
        [("1", "passed")], ["school_id", "dq_status"]
    )
    with (
        patch("src.assets.school_connectivity.assets.get_schema_columns_datahub"),
        patch(
            "src.assets.school_connectivity.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        result = await qos_school_connectivity_gold(
            context=op_context,
            qos_school_connectivity_dq_passed_rows=passed_df,
            config=mock_file_config,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_school_connectivity_realtime_schools(
    mock_file_config, spark_session, mock_adls_client, op_context
):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    updated_schools_df = spark_session.createDataFrame(
        [("1", "1001", "yes", "source", datetime(2023, 1, 1), "BRA")],
        [
            "school_id_giga",
            "school_id_govt",
            "connectivity_RT",
            "connectivity_RT_datasource",
            "connectivity_RT_ingestion_timestamp",
            "country_code",
        ],
    )
    current_df = spark_session.createDataFrame(
        [("2", "1002", "no", datetime(2022, 1, 1), "source_old", "BRA")],
        [
            "school_id_giga",
            "school_id_govt",
            "connectivity_RT",
            "connectivity_RT_ingestion_timestamp",
            "connectivity_RT_datasource",
            "country_code",
        ],
    )
    with (
        patch(
            "src.assets.school_connectivity.assets.get_all_connectivity_rt_schools"
        ) as mock_get_schools,
        patch(
            "src.assets.school_connectivity.assets.check_table_exists",
            return_value=True,
        ),
        patch("src.assets.school_connectivity.assets.DeltaTable") as mock_dt_class,
        patch("src.assets.school_connectivity.assets.create_schema"),
        patch("src.assets.school_connectivity.assets.create_delta_table"),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
    ):
        mock_get_schools.return_value = updated_schools_df
        mock_dt_instance = MagicMock()
        mock_dt_class.forName.return_value = mock_dt_instance
        mock_dt_instance.toDF.return_value = current_df
        mock_dt_instance.alias.return_value.merge.return_value.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.return_value = None
        result = await school_connectivity_realtime_schools(
            context=op_context,
            adls_file_client=mock_adls_client,
            spark=mock_spark_resource,
        )
        assert isinstance(result, Output)


@pytest.mark.asyncio
async def test_school_connectivity_realtime_silver(
    mock_file_config, spark_session, mock_adls_client, op_context
):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    pandas_df = pd.DataFrame(
        [
            {
                "school_id_giga": "1",
                "connectivity": "yes",
                "connectivity_RT": "yes",
                "connectivity_RT_datasource": "source",
                "connectivity_RT_ingestion_timestamp": "2023-01-01",
            }
        ]
    )
    mock_adls_client.download_csv_as_pandas_dataframe.return_value = pandas_df
    current_silver_df = spark_session.createDataFrame(
        [("1", "no", "yes", "source", datetime(2023, 1, 1))],
        [
            "school_id_giga",
            "connectivity",
            "connectivity_RT",
            "connectivity_RT_datasource",
            "connectivity_RT_ingestion_timestamp",
        ],
    )
    with (
        patch(
            "src.assets.school_connectivity.assets.check_table_exists",
            return_value=True,
        ),
        patch("src.assets.school_connectivity.assets.DeltaTable") as mock_dt_class,
        patch(
            "src.assets.school_connectivity.assets.get_schema_columns"
        ) as mock_get_columns,
        patch(
            "src.assets.school_connectivity.assets.get_primary_key",
            return_value=["school_id_giga"],
        ),
        patch(
            "src.assets.school_connectivity.assets.add_missing_columns",
            side_effect=lambda df, cols: df,
        ),
        patch(
            "src.assets.school_connectivity.assets.transform_types",
            side_effect=lambda df, *args: df,
        ),
        patch(
            "src.assets.school_connectivity.assets.full_in_cluster_merge",
            return_value=current_silver_df,
        ),
        patch(
            "src.assets.school_connectivity.assets.compute_row_hash",
            side_effect=lambda df: df,
        ),
        patch("src.assets.school_connectivity.assets.get_schema_columns_datahub"),
        patch(
            "src.assets.school_connectivity.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
        patch("pyspark.sql.catalog.Catalog.refreshTable"),
    ):
        MockCol = MagicMock()
        MockCol.name = "school_id_giga"
        mock_get_columns.return_value = [MockCol]
        mock_dt_instance = MagicMock()
        mock_dt_class.forName.return_value = mock_dt_instance
        mock_dt_instance.toDF.return_value = current_silver_df
        result = await school_connectivity_realtime_silver(
            context=op_context,
            spark=mock_spark_resource,
            config=mock_file_config,
            adls_file_client=mock_adls_client,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_school_connectivity_realtime_master(
    mock_file_config, spark_session, mock_adls_client, op_context
):
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    pandas_df = pd.DataFrame(
        [
            {
                "school_id_giga": "1",
                "connectivity": "yes",
                "connectivity_RT": "yes",
                "connectivity_RT_datasource": "source",
                "connectivity_RT_ingestion_timestamp": "2023-01-01",
            }
        ]
    )
    mock_adls_client.download_csv_as_pandas_dataframe.return_value = pandas_df
    current_master_df = spark_session.createDataFrame(
        [("1", "no", "yes", "source", datetime(2023, 1, 1))],
        [
            "school_id_giga",
            "connectivity",
            "connectivity_RT",
            "connectivity_RT_datasource",
            "connectivity_RT_ingestion_timestamp",
        ],
    )
    with (
        patch(
            "src.assets.school_connectivity.assets.check_table_exists",
            return_value=True,
        ),
        patch("src.assets.school_connectivity.assets.DeltaTable") as mock_dt_class,
        patch(
            "src.assets.school_connectivity.assets.get_schema_columns"
        ) as mock_get_columns,
        patch(
            "src.assets.school_connectivity.assets.get_primary_key",
            return_value=["school_id_giga"],
        ),
        patch(
            "src.assets.school_connectivity.assets.add_missing_columns",
            side_effect=lambda df, cols: df,
        ),
        patch(
            "src.assets.school_connectivity.assets.transform_types",
            side_effect=lambda df, *args: df,
        ),
        patch(
            "src.assets.school_connectivity.assets.full_in_cluster_merge",
            return_value=current_master_df,
        ),
        patch(
            "src.assets.school_connectivity.assets.compute_row_hash",
            side_effect=lambda df: df,
        ),
        patch("src.assets.school_connectivity.assets.get_schema_columns_datahub"),
        patch(
            "src.assets.school_connectivity.assets.datahub_emit_metadata_with_exception_catcher"
        ),
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
        patch(
            "src.assets.school_connectivity.assets.get_table_preview",
            return_value="preview",
        ),
        patch("pyspark.sql.catalog.Catalog.refreshTable"),
    ):
        MockCol = MagicMock()
        MockCol.name = "school_id_giga"
        mock_get_columns.return_value = [MockCol]
        mock_dt_instance = MagicMock()
        mock_dt_class.forName.return_value = mock_dt_instance
        mock_dt_instance.toDF.return_value = current_master_df
        result = await school_connectivity_realtime_master(
            context=op_context,
            spark=mock_spark_resource,
            config=mock_file_config,
            adls_file_client=mock_adls_client,
            school_connectivity_realtime_silver=current_master_df,
        )
        assert isinstance(result, Output)
        assert result.value.count() == 1


@pytest.mark.asyncio
async def test_qos_school_connectivity_data_quality_results_summary(
    mock_file_config, spark_session
):
    raw_df = spark_session.createDataFrame([("1",)], ["school_id"])
    dq_results_df = spark_session.createDataFrame(
        [("1", "passed")], ["school_id", "dq_status"]
    )
    mock_spark_resource = MagicMock()
    mock_spark_resource.spark_session = spark_session
    with (
        patch("src.assets.school_connectivity.assets.aggregate_report_spark_df") as _,
        patch(
            "src.assets.school_connectivity.assets.aggregate_report_json",
            return_value={"passed": 1},
        ),
        patch(
            "src.assets.school_connectivity.assets.get_output_metadata", return_value={}
        ),
    ):
        result = await qos_school_connectivity_data_quality_results_summary(
            qos_school_connectivity_raw=raw_df,
            qos_school_connectivity_data_quality_results=dq_results_df,
            spark=mock_spark_resource,
            config=mock_file_config,
        )
        assert isinstance(result, Output)
        assert result.value == {"passed": 1}


@pytest.mark.asyncio
async def test_connectivity_broadcast_master_release_notes(
    mock_file_config, spark_session, op_context
):
    mock_spark_resource = MagicMock()
    master_df = spark_session.createDataFrame([("1",)], ["school_id"])
    with (
        patch(
            "src.assets.school_connectivity.assets.send_master_release_notes"
        ) as mock_send,
        patch("src.assets.school_connectivity.assets.get_rest_emitter") as _,
        patch("src.assets.school_connectivity.assets.DatasetPatchBuilder"),
    ):
        mock_send.return_value = {
            "version": "1.0",
            "rows": 1,
            "added": 1,
            "modified": 0,
            "deleted": 0,
        }
        result = await connectivity_broadcast_master_release_notes(
            context=op_context,
            config=mock_file_config,
            spark=mock_spark_resource,
            school_connectivity_realtime_master=master_df,
        )
        assert isinstance(result, Output)
        assert result.metadata["version"].text == "1.0"


# ════════════════════════════════════════════════════════════════════════
#  BDD-style functional tests — real business logic validation
# ════════════════════════════════════════════════════════════════════════


class TestBronzeSignatureGeneration:
    """GIVEN raw connectivity data joined with silver,
    WHEN qos_school_connectivity_bronze runs,
    THEN signature, gigasync_id, and date are correctly derived."""

    @pytest.mark.asyncio
    async def test_signature_is_sha256_hash(
        self, mock_file_config, spark_session, op_context
    ):
        """GIVEN a raw row with school_id and timestamp,
        WHEN bronze runs,
        THEN signature should be a 64-char hex SHA-256 hash."""
        mock_spark_resource = MagicMock()
        mock_spark_resource.spark_session = spark_session
        raw_df = spark_session.createDataFrame(
            [("GIGA01", "2023-01-01T00:00:00")], ["school_id", "timestamp"]
        )
        silver_df = spark_session.createDataFrame([("GIGA01",)], ["school_id_giga"])

        with (
            patch("src.assets.school_connectivity.assets.DeltaTable") as mock_dt_class,
            patch.object(spark_session.catalog, "tableExists", return_value=True),
            patch(
                "src.assets.school_connectivity.assets.get_output_metadata",
                return_value={},
            ),
            patch(
                "src.assets.school_connectivity.assets.get_table_preview",
                return_value="",
            ),
        ):
            mock_dt_class.forName.return_value.toDF.return_value = silver_df
            result = await qos_school_connectivity_bronze(
                context=op_context,
                qos_school_connectivity_raw=raw_df,
                config=mock_file_config,
                spark=mock_spark_resource,
            )

        df = result.value
        assert "signature" in df.columns
        assert "gigasync_id" in df.columns
        assert "date" in df.columns

        sig = df.iloc[0]["signature"]
        giga_id = df.iloc[0]["gigasync_id"]

        # SHA-256 produces a 64-char hex string
        assert len(sig) == 64
        assert all(c in "0123456789abcdef" for c in sig)
        assert len(giga_id) == 64

    @pytest.mark.asyncio
    async def test_gigasync_id_is_deterministic(
        self, mock_file_config, spark_session, op_context
    ):
        """GIVEN two identical rows (same school_id_giga + timestamp),
        WHEN bronze runs,
        THEN they should produce the same gigasync_id and be deduplicated."""
        mock_spark_resource = MagicMock()
        mock_spark_resource.spark_session = spark_session
        raw_df = spark_session.createDataFrame(
            [
                ("GIGA01", "2023-01-01T00:00:00"),
                ("GIGA01", "2023-01-01T00:00:00"),  # duplicate
            ],
            ["school_id", "timestamp"],
        )
        silver_df = spark_session.createDataFrame([("GIGA01",)], ["school_id_giga"])

        with (
            patch("src.assets.school_connectivity.assets.DeltaTable") as mock_dt_class,
            patch.object(spark_session.catalog, "tableExists", return_value=True),
            patch(
                "src.assets.school_connectivity.assets.get_output_metadata",
                return_value={},
            ),
            patch(
                "src.assets.school_connectivity.assets.get_table_preview",
                return_value="",
            ),
        ):
            mock_dt_class.forName.return_value.toDF.return_value = silver_df
            result = await qos_school_connectivity_bronze(
                context=op_context,
                qos_school_connectivity_raw=raw_df,
                config=mock_file_config,
                spark=mock_spark_resource,
            )

        df = result.value
        # Deduplicated by gigasync_id
        assert len(df) == 1

    @pytest.mark.asyncio
    async def test_date_derived_from_timestamp(
        self, mock_file_config, spark_session, op_context
    ):
        """GIVEN a raw row with timestamp '2023-06-15T12:30:00',
        WHEN bronze runs,
        THEN date column should be '2023-06-15'."""
        mock_spark_resource = MagicMock()
        mock_spark_resource.spark_session = spark_session
        raw_df = spark_session.createDataFrame(
            [("GIGA01", "2023-06-15T12:30:00")], ["school_id", "timestamp"]
        )
        silver_df = spark_session.createDataFrame([("GIGA01",)], ["school_id_giga"])

        with (
            patch("src.assets.school_connectivity.assets.DeltaTable") as mock_dt_class,
            patch.object(spark_session.catalog, "tableExists", return_value=True),
            patch(
                "src.assets.school_connectivity.assets.get_output_metadata",
                return_value={},
            ),
            patch(
                "src.assets.school_connectivity.assets.get_table_preview",
                return_value="",
            ),
        ):
            mock_dt_class.forName.return_value.toDF.return_value = silver_df
            result = await qos_school_connectivity_bronze(
                context=op_context,
                qos_school_connectivity_raw=raw_df,
                config=mock_file_config,
                spark=mock_spark_resource,
            )

        df = result.value
        assert str(df.iloc[0]["date"]) == "2023-06-15"


class TestConnectivityDqLogic:
    """GIVEN bronze connectivity data,
    WHEN qos DQ checks run for real,
    THEN mandatory field violations produce critical errors."""

    @pytest.mark.asyncio
    async def test_null_school_id_giga_is_critical(
        self, mock_file_config, spark_session, op_context
    ):
        """GIVEN row with null school_id_giga (mandatory for qos),
        WHEN row_level_checks runs with dataset_type='qos',
        THEN dq_has_critical_error should be 1."""
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [
                StructField("school_id_giga", StringType()),
                StructField("timestamp", StringType()),
            ]
        )
        bronze_df = spark_session.createDataFrame(
            [("GIGA01", "2023-01-01"), (None, "2023-01-02")], schema
        )

        with (
            patch(
                "src.assets.school_connectivity.assets.get_output_metadata",
                return_value={},
            ),
            patch(
                "src.assets.school_connectivity.assets.get_table_preview",
                return_value="",
            ),
        ):
            result = await qos_school_connectivity_data_quality_results(
                context=op_context,
                config=mock_file_config,
                qos_school_connectivity_bronze=bronze_df,
            )

        df = result.value
        # Row with school_id_giga='GIGA01' should pass
        passed = df[df["school_id_giga"] == "GIGA01"]
        assert len(passed) == 1
        assert passed.iloc[0]["dq_has_critical_error"] == 0

        # Row with null school_id_giga should fail
        failed = df[df["school_id_giga"].isna()]
        assert len(failed) == 1
        assert failed.iloc[0]["dq_has_critical_error"] == 1

    @pytest.mark.asyncio
    async def test_dq_split_separates_passed_failed(
        self, mock_file_config, spark_session
    ):
        """GIVEN DQ results with mixed pass/fail,
        WHEN dq_split_passed/failed runs,
        THEN correct rows are in each split."""
        dq_df = spark_session.createDataFrame(
            [("GIGA01", 0), ("GIGA02", 1)],
            ["school_id_giga", "dq_has_critical_error"],
        )

        with (
            patch(
                "src.assets.school_connectivity.assets.get_output_metadata",
                return_value={},
            ),
            patch(
                "src.assets.school_connectivity.assets.get_table_preview",
                return_value="",
            ),
        ):
            passed = await qos_school_connectivity_dq_passed_rows(
                qos_school_connectivity_data_quality_results=dq_df,
                config=mock_file_config,
            )
            failed = await qos_school_connectivity_dq_failed_rows(
                qos_school_connectivity_data_quality_results=dq_df,
                config=mock_file_config,
            )

        assert len(passed.value) == 1
        assert passed.value.iloc[0]["school_id_giga"] == "GIGA01"
        assert len(failed.value) == 1
        assert failed.value.iloc[0]["school_id_giga"] == "GIGA02"
