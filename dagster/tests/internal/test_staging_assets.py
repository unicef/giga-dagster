from unittest.mock import MagicMock, patch

import pytest
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
    def test_process_staging_changes_no_silver(
        self,
        mock_exists,
        mock_pk,
        mock_cols,
        mock_context,
        mock_config,
        mock_adls_client,
        spark_session,
    ):
        mock_cols.return_value = []
        mock_pk.return_value = "id"
        mock_exists.return_value = False

        step = StagingStep(
            context=mock_context,
            config=mock_config,
            adls_file_client=mock_adls_client,
            spark=spark_session,
            change_type=StagingChangeTypeEnum.UPDATE,
        )

        step.standard_transforms = MagicMock(side_effect=lambda x: x)
        step.create_empty_staging_table = MagicMock()
        step.sync_schema_staging = MagicMock()
        step.upsert_rows = MagicMock(return_value=MagicMock())

        mock_df = MagicMock()
        mock_df.write.option.return_value.format.return_value.mode.return_value.saveAsTable = MagicMock()
        mock_df.count.return_value = 10
        step.standard_transforms = MagicMock(return_value=mock_df)

        result = step._process_staging_changes(mock_df)

        assert result is not None
        step.create_empty_staging_table.assert_called_once()


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
            patch(
                "src.internal.common_assets.staging.get_trino_context"
            ) as mock_trino_ctx,
        ):
            mock_db = MagicMock()
            mock_db_ctx.return_value.__enter__.return_value = mock_db

            mock_trino = MagicMock()
            mock_trino_ctx.return_value.__enter__.return_value = mock_trino

            mock_trino_exec = MagicMock()
            mock_trino_exec.scalar.return_value = 100
            mock_trino.execute.return_value = mock_trino_exec

            mock_req = MagicMock()
            mock_req.enabled = False
            mock_db.scalar.return_value = mock_req

            mock_update_res = MagicMock()
            mock_update_res.rowcount = 1
            mock_db.execute.return_value = mock_update_res

            staging_step._update_approval_request_status(MagicMock())

            mock_db.execute.assert_called()

    def test_update_approval_request_already_enabled(self, staging_step):
        with (
            patch("src.internal.common_assets.staging.get_db_context") as mock_db_ctx,
            patch.object(staging_step, "_get_pre_update_row_count", return_value=10),
        ):
            mock_db = MagicMock()
            mock_db_ctx.return_value.__enter__.return_value = mock_db

            mock_req = MagicMock()
            mock_req.enabled = True
            mock_db.scalar.return_value = mock_req

            staging_step._update_approval_request_status(MagicMock())

            mock_db.execute.assert_not_called()

    def test_validate_delete_cdf_success(self, staging_step):
        staging_step.change_type = StagingChangeTypeEnum.DELETE

        with patch(
            "src.internal.common_assets.staging.get_trino_context"
        ) as mock_trino:
            mock_conn = MagicMock()
            mock_trino.return_value.__enter__.return_value = mock_conn

            mock_res = MagicMock()
            mock_res.scalar.return_value = 5
            mock_conn.execute.return_value = mock_res

            staging_step._validate_delete_cdf()

    def test_validate_delete_cdf_failure(self, staging_step):
        staging_step.change_type = StagingChangeTypeEnum.DELETE

        with (
            patch("src.internal.common_assets.staging.get_trino_context") as mock_trino,
            patch("src.internal.common_assets.staging.get_db_context") as mock_db_ctx,
        ):
            mock_conn = MagicMock()
            mock_trino.return_value.__enter__.return_value = mock_conn
            mock_res = MagicMock()
            mock_res.scalar.return_value = 0
            mock_conn.execute.return_value = mock_res

            mock_db = MagicMock()
            mock_db_ctx.return_value.__enter__.return_value = mock_db

            with pytest.raises(RuntimeError, match="Delete CDF empty"):
                staging_step._validate_delete_cdf()

            mock_db.execute.assert_called()
