from src.constants import DataTier
from src.utils.schema import construct_full_table_name, construct_schema_name_for_tier


def test_construct_schema_name_silver_tier():
    result = construct_schema_name_for_tier("school_master", DataTier.SILVER)
    assert result == "school_master_silver"
    assert "silver" in result.lower()


def test_construct_schema_name_staging_tier():
    result = construct_schema_name_for_tier("school_coverage", DataTier.STAGING)
    assert result == "school_coverage_staging"
    assert "staging" in result


def test_construct_schema_name_rejected_tier():
    result = construct_schema_name_for_tier(
        "school_geolocation", DataTier.MANUAL_REJECTED
    )
    assert result == "school_geolocation_rejected"
    assert "rejected" in result


def test_construct_schema_name_gold_no_suffix():
    result = construct_schema_name_for_tier("SCHOOL_MASTER", DataTier.GOLD)
    assert result == "school_master"
    assert "_gold" not in result


def test_construct_schema_name_bronze_no_suffix():
    result = construct_schema_name_for_tier("School_Coverage", DataTier.BRONZE)
    assert result == "school_coverage"
    assert "_bronze" not in result


def test_construct_schema_name_raw_no_suffix():
    result = construct_schema_name_for_tier("DATASET", DataTier.RAW)
    assert result == "dataset"
    assert "_raw" not in result


def test_construct_schema_name_case_insensitive():
    schemas = [
        ("UPPERCASE", DataTier.SILVER),
        ("MixedCase", DataTier.STAGING),
        ("lower", DataTier.SILVER),
    ]
    for name, tier in schemas:
        result = construct_schema_name_for_tier(name, tier)
        assert result == result.lower()


def test_construct_full_table_name_basic():
    result = construct_full_table_name("SchemaName", "TableName")
    assert result == "schemaname.tablename"
    assert "." in result


def test_construct_full_table_name_complex():
    test_cases = [
        ("SCHEMA", "TABLE", "schema.table"),
        ("my_schema", "my_table", "my_schema.my_table"),
        ("S123", "T456", "s123.t456"),
    ]
    for schema, table, expected in test_cases:
        result = construct_full_table_name(schema, table)
        assert result == expected
