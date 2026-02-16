from datetime import datetime

import pytest
from src.utils.filename import (
    deconstruct_adhoc_filename_components,
    deconstruct_qos_filename_components,
    deconstruct_school_master_filename_components,
    deconstruct_unstructured_filename_components,
)


def test_deconstruct_school_master_filename_geolocation():
    filepath = "raw/uploads/geolocation/ABC_BRA_geolocation_20240516-090023.csv"
    result = deconstruct_school_master_filename_components(filepath)
    assert result.id == "ABC"
    assert result.country_code == "BRA"
    assert result.dataset_type == "geolocation"
    assert isinstance(result.timestamp, datetime)
    assert result.source is None


def test_deconstruct_school_master_filename_coverage():
    filepath = "raw/uploads/coverage/ABC_BRA_coverage_itu_20240516-090023.csv"
    result = deconstruct_school_master_filename_components(filepath)
    assert result.id == "ABC"
    assert result.country_code == "BRA"
    assert result.dataset_type == "coverage"
    assert result.source == "itu"
    assert isinstance(result.timestamp, datetime)


def test_deconstruct_school_master_filename_approved_ids():
    filepath = "BRA_geolocation_20240516-090023.json"
    result = deconstruct_school_master_filename_components(filepath)
    assert result.id == ""
    assert result.country_code == "BRA"
    assert result.dataset_type == "geolocation"
    assert isinstance(result.timestamp, datetime)


def test_deconstruct_school_master_filename_delete_ids():
    filepath = "BRA_20240516-090023.json"
    result = deconstruct_school_master_filename_components(filepath)
    assert result.id == ""
    assert result.country_code == "BRA"
    assert isinstance(result.timestamp, datetime)


def test_deconstruct_school_master_filename_invalid():
    filepath = "invalid_filename.csv"
    with pytest.raises(ValueError):
        deconstruct_school_master_filename_components(filepath)


def test_deconstruct_qos_filename():
    filepath = "raw/qos/BRA/file.json"
    result = deconstruct_qos_filename_components(filepath)
    if result:
        assert result.dataset_type == "qos"
        assert result.country_code == "BRA"


def test_deconstruct_qos_filename_with_qos_in_stem():
    filepath = "raw/data/BRA/qos_file.json"
    result = deconstruct_qos_filename_components(filepath)
    if result:
        assert result.dataset_type == "qos"
        assert result.country_code == "BRA"


def test_deconstruct_adhoc_filename_with_qos():
    filepath = "gold/qos/BRA/file.csv"
    result = deconstruct_adhoc_filename_components(filepath)
    assert result.dataset_type == "qos"
    assert result.country_code == "BRA"


def test_deconstruct_adhoc_filename_with_country_in_stem():
    filepath = "gold/BRA_school_master.csv"
    result = deconstruct_adhoc_filename_components(filepath)
    assert result.country_code == "BRA"


def test_deconstruct_adhoc_filename_with_country_in_parent():
    filepath = "gold/BRA/file.csv"
    result = deconstruct_adhoc_filename_components(filepath)
    assert result.country_code == "BRA"


def test_deconstruct_adhoc_filename_no_country():
    filepath = "gold/invalid_file.csv"
    result = deconstruct_adhoc_filename_components(filepath)
    assert result is None


def test_deconstruct_unstructured_filename():
    filepath = "raw/uploads/unstructured/PHL/mrm67tmhy5fhuh34q9bc81pr_PHL_unstructured_20240516-090023.png"
    result = deconstruct_unstructured_filename_components(filepath)
    assert result.id == "mrm67tmhy5fhuh34q9bc81pr"
    assert result.country_code == "PHL"
    assert result.dataset_type == "unstructured"
    assert isinstance(result.timestamp, datetime)


def test_deconstruct_unstructured_filename_na_country():
    filepath = "raw/uploads/unstructured/N-A/id_N-A_unstructured_20240516-090023.png"
    result = deconstruct_unstructured_filename_components(filepath)
    assert result.country_code == "N/A"
