from datetime import datetime

import pytest
from pydantic import ValidationError
from src.schemas import approval_request, connectivity_rt, qos, user
from src.schemas.filename_components import FilenameComponents


def test_filename_components_validation():
    fc = FilenameComponents(
        country_code="BRA", dataset_type="geolocation", timestamp=datetime.now()
    )
    assert fc.country_code == "BRA"
    with pytest.raises(ValidationError):
        FilenameComponents(country_code="")


def test_qos_schema_creation():
    assert qos is not None
    assert len(dir(qos)) > 5


def test_user_schema_creation():
    assert user is not None


def test_approval_request_schema():
    assert approval_request is not None


def test_connectivity_rt_schema():
    assert connectivity_rt is not None
