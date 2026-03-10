from src.assets.adhoc import health_master_csv_to_gold
from src.assets.adhoc.health_master_csv_to_gold import (
    ADLSFileClient,
    PySparkResource,
    add_missing_columns,
    adhoc__health_master_data_transforms,
    adhoc__load_health_master_csv,
    adhoc__publish_health_master_to_gold,
    f,
    get_schema_columns,
)


def test_module_imports():
    assert health_master_csv_to_gold is not None


def test_adhoc_load_health_master_csv_exists():
    assert callable(adhoc__load_health_master_csv)


def test_adhoc_health_master_data_transforms_exists():
    assert callable(adhoc__health_master_data_transforms)


def test_adhoc_publish_health_master_to_gold_exists():
    assert callable(adhoc__publish_health_master_to_gold)


def test_imports_pyspark_resource():
    assert PySparkResource is not None


def test_imports_spark_functions():
    assert f is not None


def test_imports_adls_client():
    assert ADLSFileClient is not None


def test_imports_schema_utils():
    assert callable(get_schema_columns)


def test_imports_transform_functions():
    assert callable(add_missing_columns)
