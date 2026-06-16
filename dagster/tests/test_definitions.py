from src.definitions import defs

from dagster import Definitions


def test_definitions_loaded():
    assert isinstance(defs, Definitions)
    assert defs is not None
