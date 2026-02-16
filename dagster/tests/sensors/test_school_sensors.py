from unittest.mock import MagicMock, patch

from src.sensors.school_coverage import (
    school_master_coverage__admin_delete_rows_sensor,
    school_master_coverage__post_manual_checks_sensor,
    school_master_coverage__raw_file_uploads_sensor,
)

from dagster import RunRequest, SkipReason, build_sensor_context


@patch("src.sensors.school_coverage.deconstruct_school_master_filename_components")
def test_school_master_coverage__raw_file_uploads_sensor(
    mock_deconstruct, mock_adls_client
):
    file_data = MagicMock()
    file_data.is_directory = False
    file_data.name = "upload/school-coverage/BRA/file.csv"

    mock_adls_client.list_paths_generator.return_value = iter([file_data])

    mock_adls_client.fetch_metadata_for_blob.return_value = {"meta": "data"}
    mock_adls_client.get_file_metadata.return_value.size = 123

    mock_comps = MagicMock()
    mock_comps.country_code = "BRA"
    mock_deconstruct.return_value = mock_comps

    context = build_sensor_context()
    results = list(
        school_master_coverage__raw_file_uploads_sensor(
            context=context, adls_file_client=mock_adls_client
        )
    )

    assert len(results) > 0
    assert isinstance(results[0], RunRequest)
    assert results[0].tags["country"] == "BRA"


@patch("src.sensors.school_coverage.deconstruct_school_master_filename_components")
def test_school_master_coverage__post_manual_checks_sensor(
    mock_deconstruct, mock_adls_client
):
    file_data = MagicMock()
    file_data.is_directory = False
    file_data.name = "staging/approved/file.csv"
    mock_adls_client.list_paths_generator.return_value = iter([file_data])

    mock_comps = MagicMock()
    mock_comps.country_code = "BRA"
    mock_deconstruct.return_value = mock_comps

    context = build_sensor_context()
    results = list(
        school_master_coverage__post_manual_checks_sensor(
            context=context, adls_file_client=mock_adls_client
        )
    )
    assert len(results) > 0
    assert isinstance(results[0], RunRequest)


@patch("src.sensors.school_coverage.deconstruct_school_master_filename_components")
def test_school_master_coverage__admin_delete_rows_sensor(
    mock_deconstruct, mock_adls_client
):
    file_data = MagicMock()
    file_data.is_directory = False
    file_data.name = "staging/delete/file.csv"
    mock_adls_client.list_paths_generator.return_value = iter([file_data])

    mock_comps = MagicMock()
    mock_comps.country_code = "BRA"
    mock_deconstruct.return_value = mock_comps

    context = build_sensor_context()
    results = list(
        school_master_coverage__admin_delete_rows_sensor(
            context=context, adls_file_client=mock_adls_client
        )
    )
    assert len(results) > 0
    assert isinstance(results[0], RunRequest)


def test_sensor_skips_if_empty(mock_adls_client):
    mock_adls_client.list_paths_generator.return_value = iter([])
    context = build_sensor_context()
    results = list(
        school_master_coverage__raw_file_uploads_sensor(
            context=context, adls_file_client=mock_adls_client
        )
    )
    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
