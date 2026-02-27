from src.utils import db
from src.utils.db import base, primary


def test_primary_db_import():
    assert primary is not None


def test_db_base_exists():
    assert base is not None


def test_db_init_exists():
    assert db is not None
