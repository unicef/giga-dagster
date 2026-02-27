from unittest.mock import MagicMock, patch

from src.sensors.qos_availability import (
    qos_availability__raw_file_uploads_sensor,
)

from dagster import RunRequest, SkipReason, build_sensor_context


def test_qos_availability_sensor_yields_request(mock_adls_client):
    file_data = MagicMock()
    file_data.is_directory = False
    file_data.name = "upload/qos-availability/BRA/test_file.csv"

    mock_adls_client.list_paths_generator.return_value = iter([file_data])
    mock_adls_client.fetch_metadata_for_blob.return_value = {"meta": "data"}
    mock_adls_client.get_file_metadata.return_value.size = 1024

    context = build_sensor_context()

    with patch(
        "src.sensors.qos_availability.deconstruct_adhoc_filename_components"
    ) as mock_decomp:
        mock_decomp.return_value.country_code = "BRA"

        results = list(
            qos_availability__raw_file_uploads_sensor(
                context=context, adls_file_client=mock_adls_client
            )
        )

        assert len(results) == 1
        assert isinstance(results[0], RunRequest)
        assert results[0].run_key == "upload/qos-availability/BRA/test_file.csv"
        assert results[0].tags["country"] == "BRA"

        ops_config = results[0].run_config["ops"]
        assert "qos_availability_raw" in ops_config
        assert "qos_availability_transforms" in ops_config
        assert "publish_qos_availability_to_gold" in ops_config


def test_qos_availability_sensor_skips(mock_adls_client):
    mock_adls_client.list_paths_generator.return_value = iter([])
    context = build_sensor_context()

    results = list(
        qos_availability__raw_file_uploads_sensor(
            context=context, adls_file_client=mock_adls_client
        )
    )

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
