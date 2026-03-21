from datetime import datetime
from unittest.mock import MagicMock, patch

from src.sensors.adhoc import (
    custom_dataset_sensor,
    health_master__gold_csv_to_deltatable_sensor,
    school_master__gold_csv_to_deltatable_sensor,
    school_qos_raw__gold_csv_to_deltatable_sensor,
)

from dagster import RunRequest


def test_school_master_gold_sensor(mock_context, mock_adls_client):
    func = school_master__gold_csv_to_deltatable_sensor
    if hasattr(func, "__wrapped__"):
        func = func.__wrapped__

    mock_file = MagicMock()
    mock_file.name = "country=TST/test_file.csv"
    mock_file.path = "country=TST/test_file.csv"
    mock_file.is_directory = False

    client = mock_adls_client
    client.list_paths.return_value = [mock_file]
    client.list_paths_generator.return_value = [mock_file]

    client.fetch_metadata_for_blob.return_value = {}
    client.get_file_metadata.return_value.size = 100
    client.get_file_metadata.return_value.last_modified = datetime(2023, 1, 1)

    with patch("src.sensors.adhoc.deconstruct_adhoc_filename_components") as mock_decon:
        mock_decon.return_value.country_code = "TST"

        gen = func(mock_context, client)
        res = list(gen)

        assert len(res) == 2
        assert isinstance(res[0], RunRequest)
        assert res[0].tags["country"] == "TST"


def test_health_master_gold_sensor(mock_context, mock_adls_client):
    func = health_master__gold_csv_to_deltatable_sensor
    if hasattr(func, "__wrapped__"):
        func = func.__wrapped__

    mock_file = MagicMock()
    mock_file.name = "path/to/TST/test_file.csv"
    mock_file.is_directory = False

    mock_adls_client.list_paths_generator.return_value = [mock_file]
    mock_adls_client.get_file_metadata.return_value.last_modified = datetime(2023, 1, 1)

    gen = func(mock_context, mock_adls_client)
    res = list(gen)

    assert len(res) == 1
    assert isinstance(res[0], RunRequest)
    assert res[0].tags["country"] == "TST"


def test_school_qos_raw_sensor(mock_context, mock_adls_client):
    func = school_qos_raw__gold_csv_to_deltatable_sensor
    if hasattr(func, "__wrapped__"):
        func = func.__wrapped__

    mock_file = MagicMock()
    mock_file.name = "path/to/TST/test_file.csv"
    mock_file.is_directory = False

    mock_adls_client.list_paths_generator.return_value = [mock_file]

    gen = func(mock_context, mock_adls_client)
    res = list(gen)

    assert len(res) == 1
    assert isinstance(res[0], RunRequest)
    assert res[0].tags["country"] == "TST"


def test_custom_dataset_sensor(mock_context, mock_adls_client):
    func = custom_dataset_sensor
    if hasattr(func, "__wrapped__"):
        func = func.__wrapped__

    mock_file = MagicMock()
    mock_file.name = "path/to/TST.csv"
    mock_file.is_directory = False

    mock_adls_client.list_paths_generator.return_value = [mock_file]
    mock_adls_client.get_file_metadata.return_value.last_modified = datetime(2023, 1, 1)

    gen = func(mock_context, mock_adls_client)
    res = list(gen)

    assert len(res) == 1
    assert isinstance(res[0], RunRequest)
