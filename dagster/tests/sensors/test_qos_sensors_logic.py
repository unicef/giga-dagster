from unittest.mock import MagicMock, patch

import pytest
from src.sensors.qos import (
    SchoolList,
    qos_school_list__new_apis_sensor,
)

from dagster import RunRequest, SkipReason


@pytest.fixture
def mock_db_context():
    with patch("src.sensors.qos.get_db_context") as mock_ctx:
        mock_session = MagicMock()
        mock_ctx.return_value.__enter__.return_value = mock_session
        yield mock_session


def test_qos_school_list_sensor_no_apis(mock_db_context):
    mock_db_context.query.return_value.filter.return_value.all.return_value = []

    res = list(qos_school_list__new_apis_sensor(None))

    assert len(res) == 1
    assert isinstance(res[0], SkipReason)


def test_qos_school_list_sensor_with_apis(mock_db_context):
    mock_api = MagicMock(spec=SchoolList)
    mock_api.enabled = True
    mock_api.country = "TST"
    mock_api.name = "Test API"
    mock_api.__dict__ = {
        "enabled": True,
        "country": "TST",
        "name": "Test API",
        "url": "http://test",
        "frequency": "daily",
        "environment": "dev",
    }

    with patch("src.sensors.qos.SchoolListConfig") as mock_config_cls:
        mock_config_instance = MagicMock()
        mock_config_instance.country = "TST"
        mock_config_instance.name = "Test API"
        mock_config_instance.json.return_value = "{}"
        mock_config_cls.return_value = mock_config_instance

        mock_db_context.query.return_value.filter.return_value.all.return_value = [
            mock_api
        ]

        with patch("src.sensors.qos.generate_run_ops", return_value={"op": "config"}):
            res = list(qos_school_list__new_apis_sensor())

            assert len(res) == 1
            assert isinstance(res[0], RunRequest)
            assert res[0].tags["country"] == "TST"
