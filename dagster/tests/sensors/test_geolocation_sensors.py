from unittest.mock import MagicMock, patch

from src.sensors.school_geolocation import (
    school_master_geolocation__admin_delete_rows_sensor,
    school_master_geolocation__post_manual_checks_sensor,
    school_master_geolocation__raw_file_uploads_sensor,
)

from dagster import RunRequest, SkipReason


def test_raw_file_uploads_sensor(mock_context, mock_adls_client):
    mock_file = MagicMock()
    mock_file.name = "country=TST/test_file.csv"
    mock_file.is_directory = False
    mock_adls_client.list_paths_generator.return_value = [mock_file]
    mock_adls_client.fetch_metadata_for_blob.return_value = {"key": "val"}
    mock_adls_client.get_file_metadata.return_value.size = 100

    with patch(
        "src.sensors.school_geolocation.deconstruct_school_master_filename_components"
    ) as mock_decon:
        mock_decon.return_value.country_code = "TST"

        func = school_master_geolocation__raw_file_uploads_sensor
        if hasattr(func, "__wrapped__"):
            func = func.__wrapped__

        gen = func(mock_context, mock_adls_client)
        res = list(gen)

        assert len(res) == 1
        assert isinstance(res[0], RunRequest)
        assert res[0].tags["country"] == "TST"


def test_raw_file_uploads_sensor_empty(mock_context, mock_adls_client):
    mock_adls_client.list_paths_generator.return_value = []

    func = school_master_geolocation__raw_file_uploads_sensor
    if hasattr(func, "__wrapped__"):
        func = func.__wrapped__

    gen = func(mock_context, mock_adls_client)
    res = list(gen)
    assert len(res) == 1
    assert isinstance(res[0], SkipReason)


def test_post_manual_checks_sensor(mock_context, mock_adls_client):
    mock_file = MagicMock()
    mock_file.name = "country=TST/test_file.csv"
    mock_file.is_directory = False
    mock_adls_client.list_paths_generator.return_value = [mock_file]

    with patch(
        "src.sensors.school_geolocation.deconstruct_school_master_filename_components"
    ) as mock_decon:
        mock_decon.return_value.country_code = "TST"

        func = school_master_geolocation__post_manual_checks_sensor
        if hasattr(func, "__wrapped__"):
            func = func.__wrapped__

        gen = func(mock_context, mock_adls_client)
        res = list(gen)

        assert len(res) == 1
        assert isinstance(res[0], RunRequest)


def test_admin_delete_rows_sensor(mock_context, mock_adls_client):
    mock_file = MagicMock()
    mock_file.name = "country=TST/test_file.csv"
    mock_file.is_directory = False
    mock_adls_client.list_paths_generator.return_value = [mock_file]

    with patch(
        "src.sensors.school_geolocation.deconstruct_school_master_filename_components"
    ) as mock_decon:
        mock_decon.return_value.country_code = "TST"

        func = school_master_geolocation__admin_delete_rows_sensor
        if hasattr(func, "__wrapped__"):
            func = func.__wrapped__

        gen = func(mock_context, mock_adls_client)
        res = list(gen)

        assert len(res) == 1
        assert isinstance(res[0], RunRequest)
