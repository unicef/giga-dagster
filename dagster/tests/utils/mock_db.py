from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock


def create_mock_file_upload(
    upload_id, country_code, dataset, original_filename, column_mapping=None
):
    mock = MagicMock()
    mock.id = upload_id
    mock.country = country_code
    mock.dataset = dataset
    mock.filename = original_filename
    mock.original_filename = original_filename
    mock.column_to_schema_mapping = column_mapping or {}

    # Add fields required by FileUploadConfig
    mock.created = datetime(2024, 1, 1)
    mock.uploader_id = "test_uploader"
    mock.uploader_email = "test@example.com"
    mock.dq_report_path = "raw/dq/report.json"
    mock.source = "test_source"
    mock.upload_path = f"raw/uploads/{dataset}/{country_code}/{original_filename}"
    mock.column_license = {}
    return mock


@contextmanager
def mock_db_context_provider(uploads_dict=None):
    if uploads_dict is None:
        uploads_dict = {}

    session = MagicMock()

    def scalar_side_effect(stmt):
        # Return the first upload object as a default behavior for tests
        if uploads_dict:
            return list(uploads_dict.values())[0]
        return None

    session.scalar.side_effect = scalar_side_effect
    yield session
