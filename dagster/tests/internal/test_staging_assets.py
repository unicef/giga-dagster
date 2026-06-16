from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StructType
from src.internal.common_assets.staging import (
    StagingChangeTypeEnum,
    StagingStep,
    get_files_for_review,
)
from src.utils.op_config import FileConfig


@pytest.fixture
def mock_config():
    config = MagicMock(spec=FileConfig)
    config.metastore_schema = "test_schema"
    config.country_code = "TST"
    config.dataset_type = "master"
    config.datahub_destination_dataset_urn = "urn:li:dataset:test"
    config.filepath = "test_file.csv"
    config.filepath_object = MagicMock()
    config.filepath_object.parent = "parent_path"
    config.filename_components.id = "123"
    return config


class TestStagingStep:
    @patch("src.internal.common_assets.staging.get_schema_columns")
    @patch("src.internal.common_assets.staging.get_primary_key")
    def test_init(
        self,
        mock_pk,
        mock_cols,
        mock_context,
        mock_config,
        mock_adls_client,
        spark_session,
    ):
        mock_cols.return_value = []
        mock_pk.return_value = "id"

        step = StagingStep(
            context=mock_context,
            config=mock_config,
            adls_file_client=mock_adls_client,
            spark=spark_session,
            change_type=StagingChangeTypeEnum.UPDATE,
        )

        assert step.schema_name == "test_schema"
        assert step.country_code == "TST"
        assert step.silver_table_name.endswith(".tst")

    @patch("src.internal.common_assets.staging.get_schema_columns")
    @patch("src.internal.common_assets.staging.get_primary_key")
    @patch("src.internal.common_assets.staging.check_table_exists")
    @patch("src.internal.common_assets.staging.compute_row_hash")
    def test_build_upsert_records_no_silver(
        self,
        mock_hash,
        mock_exists,
        mock_pk,
        mock_cols,
        mock_context,
        mock_config,
        mock_adls_client,
        spark_session,
    ):
        from pyspark.sql.types import StringType, StructField

        mock_cols.return_value = [
            StructField("id", StringType()),
            StructField("name", StringType()),
        ]
        mock_pk.return_value = "id"
        mock_exists.return_value = False
        mock_hash.side_effect = lambda df: df

        step = StagingStep(
            context=mock_context,
            config=mock_config,
            adls_file_client=mock_adls_client,
            spark=spark_session,
            change_type=StagingChangeTypeEnum.UPDATE,
        )

        step._get_uploaded_columns = MagicMock(return_value=["id", "name"])
        # Mock _prepare_df to avoid complex transforms
        step._prepare_df = MagicMock(side_effect=lambda df: df)

        mock_df = spark_session.createDataFrame([("1", "A")], ["id", "name"])

        result = step._build_upsert_records(mock_df)

        assert result is not None
        assert "change_type" in result.columns
        assert result.filter(result.change_type == "INSERT").count() == 1


def test_get_files_for_review(mock_adls_client, mock_config):
    f1 = MagicMock()
    f1.name = "other.csv"
    f1.last_modified = 100

    f2 = MagicMock()
    f2.name = "test_file.csv"
    f2.last_modified = 200

    mock_adls_client.list_paths.return_value = [f2, f1]

    files = get_files_for_review(mock_adls_client, mock_config, skip_current_file=True)
    assert len(files) == 1
    assert files[0].name == "other.csv"

    files_all = get_files_for_review(
        mock_adls_client, mock_config, skip_current_file=False
    )
    assert len(files_all) == 2


class TestStagingStepApproval:
    @pytest.fixture
    def staging_step(self, mock_context, mock_config, mock_adls_client, spark_session):
        with (
            patch(
                "src.internal.common_assets.staging.get_schema_columns", return_value=[]
            ),
            patch(
                "src.internal.common_assets.staging.get_primary_key", return_value="id"
            ),
            patch(
                "src.internal.common_assets.staging.check_table_exists",
                return_value=True,
            ),
        ):
            step = StagingStep(
                context=mock_context,
                config=mock_config,
                adls_file_client=mock_adls_client,
                spark=spark_session,
                change_type=StagingChangeTypeEnum.UPDATE,
            )
            step.staging_table_name = "staging_table"
            return step

    def test_update_approval_request_status_enabled(self, staging_step):
        with (
            patch("src.internal.common_assets.staging.get_db_context") as mock_db_ctx,
            patch("src.internal.common_assets.staging.DeltaTable") as mock_dt,
        ):
            # Mock DeltaTable to return actionable rows
            mock_df = MagicMock()
            mock_df.filter.return_value.count.return_value = 1
            mock_dt.forName.return_value.toDF.return_value = mock_df

            mock_db = MagicMock()
            mock_db_ctx.return_value.__enter__.return_value = mock_db

            mock_req = MagicMock()
            mock_req.enabled = False
            mock_db.scalar.return_value = mock_req

            mock_update_res = MagicMock()
            mock_update_res.rowcount = 1
            mock_db.execute.return_value = mock_update_res

            staging_step._update_approval_request_status()

            mock_db.execute.assert_called()

    def test_update_approval_request_already_enabled(self, staging_step):
        with (
            patch("src.internal.common_assets.staging.get_db_context") as mock_db_ctx,
            patch("src.internal.common_assets.staging.DeltaTable") as mock_dt,
        ):
            # Mock DeltaTable to return actionable rows
            mock_df = MagicMock()
            mock_df.filter.return_value.count.return_value = 1
            mock_dt.forName.return_value.toDF.return_value = mock_df

            mock_db = MagicMock()
            mock_db_ctx.return_value.__enter__.return_value = mock_db

            mock_req = MagicMock()
            mock_req.enabled = True
            mock_db.scalar.return_value = mock_req

            staging_step._update_approval_request_status()

            mock_db.execute.assert_not_called()

    @patch("src.internal.common_assets.staging.DeltaTable")
    def test_build_delete_records_success(self, mock_dt, staging_step):
        staging_step.change_type = StagingChangeTypeEnum.DELETE

        with patch.object(StagingStep, "silver_table_exists", new=True):
            mock_silver_df = staging_step.spark.createDataFrame([("1",)], ["id"])
            mock_dt.forName.return_value.toDF.return_value = mock_silver_df

            result = staging_step._build_delete_records(["1"])

            assert result is not None
            assert result.filter(result.change_type == "DELETE").count() == 1

    @patch("src.internal.common_assets.staging.DeltaTable")
    def test_build_delete_records_empty(self, mock_dt, staging_step):
        staging_step.change_type = StagingChangeTypeEnum.DELETE

        from pyspark.sql.types import StringType, StructField

        schema = StructType([StructField("id", StringType())])

        with patch.object(StagingStep, "silver_table_exists", new=True):
            mock_silver_df = staging_step.spark.createDataFrame([], schema)
            mock_dt.forName.return_value.toDF.return_value = mock_silver_df

            result = staging_step._build_delete_records(["1"])

            assert result is None
