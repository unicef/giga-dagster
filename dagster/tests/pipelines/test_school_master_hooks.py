from pathlib import Path
from unittest.mock import MagicMock, patch

from src.hooks.school_master import (
    school_dq_checks_location_db_update_hook,
    school_dq_overall_location_db_update_hook,
    school_ingest_error_db_update_hook,
)


class TestSchoolDQChecksLocationDBUpdateHook:
    @patch("src.hooks.school_master.FileConfig")
    @patch("src.hooks.school_master.get_db_context")
    def test_updates_db_when_step_ends_with_data_quality_results_summary(
        self, mock_db_context, mock_file_config
    ):
        mock_db = MagicMock()
        mock_db_context.return_value.__enter__.return_value = mock_db

        mock_context = MagicMock()
        mock_context.step_key = "geolocation_data_quality_results_summary"
        mock_context.log = MagicMock()

        mock_config_instance = MagicMock()
        mock_config_instance.filename_components.id = 123
        mock_config_instance.destination_filepath_object = Path(
            "/test/path/report.json"
        )
        mock_file_config.return_value = mock_config_instance

        mock_context.op_config = {"test": "config"}

        school_dq_checks_location_db_update_hook.decorated_fn(mock_context)

        assert mock_db.execute.called
        assert mock_db.commit.called
        mock_context.log.info.assert_any_call(
            "Running database update hook for DQ checks location..."
        )
        mock_context.log.info.assert_any_call("Database update hook OK")

    @patch("src.hooks.school_master.get_db_context")
    def test_skips_when_step_does_not_end_with_data_quality_results_summary(
        self, mock_db_context
    ):
        mock_db = MagicMock()
        mock_db_context.return_value.__enter__.return_value = mock_db

        mock_context = MagicMock()
        mock_context.step_key = "some_other_step"

        school_dq_checks_location_db_update_hook.decorated_fn(mock_context)

        assert not mock_db.execute.called


class TestSchoolDQOverallLocationDBUpdateHook:
    @patch("src.hooks.school_master.FileConfig")
    @patch("src.hooks.school_master.get_db_context")
    def test_updates_db_when_step_ends_with_data_quality_results(
        self, mock_db_context, mock_file_config
    ):
        mock_db = MagicMock()
        mock_db_context.return_value.__enter__.return_value = mock_db

        mock_context = MagicMock()
        mock_context.step_key = "geolocation_data_quality_results"
        mock_context.log = MagicMock()

        mock_config_instance = MagicMock()
        mock_config_instance.filename_components.id = 456
        mock_config_instance.destination_filepath_object = Path(
            "/test/path/full_results.parquet"
        )
        mock_file_config.return_value = mock_config_instance

        mock_context.op_config = {"test": "config"}

        school_dq_overall_location_db_update_hook.decorated_fn(mock_context)

        assert mock_db.execute.called
        assert mock_db.commit.called
        mock_context.log.info.assert_any_call(
            "Running database update hook for full DQ results location..."
        )
        mock_context.log.info.assert_any_call("Database update hook OK")

    @patch("src.hooks.school_master.get_db_context")
    def test_skips_when_step_does_not_end_with_data_quality_results(
        self, mock_db_context
    ):
        mock_db = MagicMock()
        mock_db_context.return_value.__enter__.return_value = mock_db

        mock_context = MagicMock()
        mock_context.step_key = "some_other_step"

        school_dq_overall_location_db_update_hook.decorated_fn(mock_context)

        assert not mock_db.execute.called


class TestSchoolIngestErrorDBUpdateHook:
    @patch("src.hooks.school_master.FileConfig")
    @patch("src.hooks.school_master.get_db_context")
    def test_updates_db_on_failure_for_non_staging_steps(
        self, mock_db_context, mock_file_config
    ):
        mock_db = MagicMock()
        mock_db_context.return_value.__enter__.return_value = mock_db

        mock_context = MagicMock()
        mock_context.step_key = "geolocation_bronze"
        mock_context.log = MagicMock()

        mock_config_instance = MagicMock()
        mock_config_instance.filename_components.id = 789
        mock_file_config.return_value = mock_config_instance

        mock_context.op_config = {"test": "config"}

        school_ingest_error_db_update_hook.decorated_fn(mock_context)

        assert mock_db.execute.called
        mock_context.log.info.assert_called_with(
            "Running database update hook for failed DQ results status..."
        )

    @patch("src.hooks.school_master.get_db_context")
    def test_skips_when_step_ends_with_staging(self, mock_db_context):
        mock_db = MagicMock()
        mock_db_context.return_value.__enter__.return_value = mock_db

        mock_context = MagicMock()
        mock_context.step_key = "geolocation_staging"

        school_ingest_error_db_update_hook.decorated_fn(mock_context)

        assert not mock_db.execute.called
