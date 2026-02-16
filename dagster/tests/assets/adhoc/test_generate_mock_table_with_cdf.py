from src.assets.adhoc import generate_mock_table_with_cdf
from src.assets.adhoc.generate_mock_table_with_cdf import (
    adhoc__copy_original,
    adhoc__generate_v2,
    adhoc__generate_v3,
)


def test_adhoc_copy_original_constants():
    assert generate_mock_table_with_cdf.SOURCE_TABLE_NAME == "school_master.ben"
    assert generate_mock_table_with_cdf.ZCDF_TABLE_NAME == "school_master.zcdf"


def test_adhoc_copy_original_asset_exists():
    assert callable(adhoc__copy_original)


def test_adhoc_generate_v2_asset_exists():
    assert callable(adhoc__generate_v2)


def test_adhoc_generate_v3_asset_exists():
    assert callable(adhoc__generate_v3)
