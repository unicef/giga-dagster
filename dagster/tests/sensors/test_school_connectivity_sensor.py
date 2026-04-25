from unittest.mock import MagicMock

from src.sensors.school_connectivity import (
    school_connectivity_update_schools_connectivity_sensor,
)

from dagster import RunRequest, SkipReason, build_sensor_context


def test_school_connectivity_sensor_yields_request(mock_adls_client):
    file_data = MagicMock()
    file_data.is_directory = False
    file_data.name = "upload/school-connectivity/BRA_connectivity.csv"

    mock_adls_client.list_paths_generator.return_value = iter([file_data])
    mock_adls_client.fetch_metadata_for_blob.return_value = {"meta": "data"}
    mock_adls_client.get_file_metadata.return_value.size = 2048

    context = build_sensor_context()

    results = list(
        school_connectivity_update_schools_connectivity_sensor(
            context=context, adls_file_client=mock_adls_client
        )
    )

    assert len(results) == 1
    assert isinstance(results[0], RunRequest)
    assert results[0].run_key == "upload/school-connectivity/BRA_connectivity.csv"
    assert results[0].tags["country"] == "BRA"

    ops_config = results[0].run_config["ops"]
    assert "school_connectivity_realtime_silver" in ops_config
    assert "school_connectivity_realtime_master" in ops_config
    assert "connectivity_broadcast_master_release_notes" in ops_config


def test_school_connectivity_sensor_skips(mock_adls_client):
    mock_adls_client.list_paths_generator.return_value = iter([])
    context = build_sensor_context()

    results = list(
        school_connectivity_update_schools_connectivity_sensor(
            context=context, adls_file_client=mock_adls_client
        )
    )

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
