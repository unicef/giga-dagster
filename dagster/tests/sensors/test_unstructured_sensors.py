from unittest.mock import MagicMock, patch

from src.sensors.unstructured import (
    generalized_unstructured__emit_metadata_to_datahub_sensor,
    unstructured__emit_metadata_to_datahub_sensor,
)

from dagster import RunRequest, SkipReason


def test_unstructured_sensor_no_files(mock_context, mock_adls_client):
    mock_adls_client.list_paths_generator.return_value = []

    func = unstructured__emit_metadata_to_datahub_sensor
    if hasattr(func, "__wrapped__"):
        func = func.__wrapped__

    res = list(func(mock_context, mock_adls_client))

    assert len(res) == 1
    assert isinstance(res[0], SkipReason)


def test_unstructured_sensor_with_file(mock_context, mock_adls_client):
    mock_file = MagicMock()
    mock_file.is_directory = False
    mock_file.name = "unstructured/BRA/file.pdf"

    mock_adls_client.list_paths_generator.return_value = [mock_file]
    mock_adls_client.fetch_metadata_for_blob.return_value = {"meta": "data"}
    mock_adls_client.get_file_metadata.return_value.size = 100

    with patch(
        "src.sensors.unstructured.deconstruct_unstructured_filename_components"
    ) as mock_decon:
        mock_decon.return_value.country_code = "BRA"

        with patch("src.sensors.unstructured.generate_run_ops") as mock_ops:
            mock_ops.return_value = {"op": "config"}

            func = unstructured__emit_metadata_to_datahub_sensor
            if hasattr(func, "__wrapped__"):
                func = func.__wrapped__

            res = list(func(mock_context, mock_adls_client))

            assert len(res) == 1
            assert isinstance(res[0], RunRequest)
            assert res[0].run_key == "unstructured/BRA/file.pdf"


def test_generalized_unstructured_sensor_with_file(mock_context, mock_adls_client):
    mock_file = MagicMock()
    mock_file.is_directory = False
    mock_file.name = "legacy_data/file.pdf"

    mock_adls_client.list_paths_generator.return_value = [mock_file]
    mock_adls_client.fetch_metadata_for_blob.return_value = {}
    mock_adls_client.get_file_metadata.return_value.size = 100
    mock_adls_client.get_file_metadata.return_value.last_modified.strftime.return_value = "20240101-120000"

    with patch("src.sensors.unstructured.generate_run_ops") as mock_ops:
        mock_ops.return_value = {"op": "config"}

        func = generalized_unstructured__emit_metadata_to_datahub_sensor
        if hasattr(func, "__wrapped__"):
            func = func.__wrapped__

        res = list(func(mock_context, mock_adls_client))

        assert len(res) == 1
        assert isinstance(res[0], RunRequest)
        assert "20240101-120000" in res[0].run_key
