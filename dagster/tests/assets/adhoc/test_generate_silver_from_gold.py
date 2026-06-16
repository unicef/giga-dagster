from src.assets.adhoc import generate_silver_from_gold
from src.assets.adhoc.generate_silver_from_gold import (
    DataTier,
    DeltaTable,
    add_missing_columns,
    adhoc__generate_silver_coverage_from_gold,
    adhoc__generate_silver_geolocation_from_gold,
    check_table_exists,
    compute_row_hash,
    constants,
    execute_query_with_error_handler,
    f,
    get_schema_columns,
    get_table_preview,
    transform_types,
)


def test_module_imports():
    assert generate_silver_from_gold is not None


def test_adhoc_generate_silver_geolocation_from_gold_exists():
    assert callable(adhoc__generate_silver_geolocation_from_gold)


def test_adhoc_generate_silver_coverage_from_gold_exists():
    assert callable(adhoc__generate_silver_coverage_from_gold)


def test_imports_delta_table():
    assert DeltaTable is not None


def test_imports_spark_functions():
    assert f is not None


def test_imports_constants():
    assert DataTier is not None
    assert constants is not None


def test_imports_transform_functions():
    assert callable(add_missing_columns)


def test_imports_delta_utils():
    assert callable(check_table_exists)
    assert callable(execute_query_with_error_handler)


def test_imports_metadata_utils():
    assert callable(get_table_preview)


def test_imports_schema_utils():
    assert callable(get_schema_columns)


def test_imports_spark_utils():
    assert callable(compute_row_hash)
    assert callable(transform_types)
