from datetime import UTC, datetime

from models.file_upload import FileUpload
from src.utils.filename import deconstruct_school_master_filename_components


def test_geolocation_upload_filename_and_path_are_stable():
    upload = FileUpload(
        id="upload-123",
        created=datetime(2026, 10, 7, 15, 4, 5, tzinfo=UTC),
        uploader_id="user-123",
        uploader_email="user@example.com",
        country="UGA",
        dataset="geolocation",
        source="portal-source",
        original_filename="schools.final.csv",
        column_to_schema_mapping={},
        column_license={},
    )

    assert upload.filename == "upload-123_UGA_geolocation_20261007-150405.csv"
    assert (
        upload.upload_path
        == "raw/uploads/school-geolocation/UGA/"
        "upload-123_UGA_geolocation_20261007-150405.csv"
    )


def test_geolocation_filename_parser_uses_final_components_and_omits_source():
    components = deconstruct_school_master_filename_components(
        "raw/uploads/school-geolocation/UGA/"
        "prefixed_value_upload-123_UGA_geolocation_20261007-150405.csv"
    )

    assert components.id == "upload-123"
    assert components.country_code == "UGA"
    assert components.dataset_type == "geolocation"
    assert components.source is None
    assert components.timestamp == datetime(2026, 10, 7, 15, 4, 5)
