from src.data_quality_checks import (
    column_relation,
    coverage,
    create_update,
    duplicates,
    geography,
    geometry,
    precision,
    utils,
)


def test_dq_duplicates():
    assert len(dir(duplicates)) > 3


def test_dq_geography():
    assert len(dir(geography)) > 3


def test_dq_geometry():
    assert len(dir(geometry)) > 3


def test_dq_precision():
    assert len(dir(precision)) > 3


def test_dq_coverage():
    assert len(dir(coverage)) > 3


def test_dq_create_update():
    assert len(dir(create_update)) > 3


def test_dq_column_relation():
    assert len(dir(column_relation)) > 3


def test_dq_utils_module():
    assert len(dir(utils)) > 10
