import sys
from unittest.mock import MagicMock

mock_cc_module = MagicMock()
mock_cc_instance = MagicMock()
mock_data = MagicMock()
mock_iso3 = MagicMock()
mock_iso3.to_list.return_value = ["BRA", "COL"]
mock_data.__getitem__.return_value = mock_iso3
mock_cc_instance.data = mock_data

mock_cc_module.CountryConverter.return_value = mock_cc_instance
sys.modules["country_converter"] = mock_cc_module

import importlib.util
import os

file_path = os.path.abspath("src/partitions.py")
spec = importlib.util.spec_from_file_location("src_partitions_file", file_path)
partitions_mod = importlib.util.module_from_spec(spec)
sys.modules["src_partitions_file"] = partitions_mod
spec.loader.exec_module(partitions_mod)

countries_partitions_def = partitions_mod.countries_partitions_def
from dagster import StaticPartitionsDefinition


def test_partitions():
    assert isinstance(countries_partitions_def, StaticPartitionsDefinition)
    keys = countries_partitions_def.get_partition_keys()
    assert len(keys) == 2
    assert "BRA" in keys
    assert "COL" in keys
