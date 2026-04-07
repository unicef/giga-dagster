from datetime import datetime
from unittest.mock import MagicMock, patch

from src.sensors.migrations import migrations__schema_sensor

from dagster import RunRequest


def test_migrations_schema_sensor():
    with patch("src.sensors.migrations.ADLSFileClient") as mock_adls_cls:
        mock_adls = mock_adls_cls.return_value

        mock_path_file = MagicMock()
        mock_path_file.__getitem__.return_value = "schema/file.sql"
        mock_path_file.is_directory = False

        mock_path_dir = MagicMock()
        mock_path_dir.__getitem__.return_value = "schema/folder"
        mock_path_dir.is_directory = True

        mock_adls.list_paths.return_value = [mock_path_dir, mock_path_file]

        mock_metadata = MagicMock()
        mock_metadata.last_modified = datetime(2023, 1, 1, 12, 0, 0)
        mock_adls.get_file_metadata.return_value = mock_metadata

        requests = list(migrations__schema_sensor())

        assert len(requests) == 1
        assert isinstance(requests[0], RunRequest)
