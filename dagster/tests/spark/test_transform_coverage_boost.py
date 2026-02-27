from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.spark.transform_functions import (
    add_missing_columns,
    clean_type_connectivity,
    column_mapping_rename,
    generate_uuid,
    get_connectivity_type_root,
)


def test_generate_uuid():
    uuid1 = generate_uuid("test_123")
    uuid2 = generate_uuid("test_123")
    uuid3 = generate_uuid("test_456")

    assert uuid1 == uuid2
    assert uuid1 != uuid3


def test_clean_type_connectivity_fibre():
    assert clean_type_connectivity("Fiber") == "fibre"
    assert clean_type_connectivity("FTTH") == "fibre"
    assert clean_type_connectivity("optical") == "fibre"


def test_clean_type_connectivity_cellular():
    assert clean_type_connectivity("4G") == "cellular"
    assert clean_type_connectivity("LTE") == "cellular"
    assert clean_type_connectivity("mobile") == "cellular"


def test_clean_type_connectivity_unknown():
    assert clean_type_connectivity(None) == "unknown"
    assert clean_type_connectivity("random_value") == "unknown"


def test_get_connectivity_type_root_wired():
    assert get_connectivity_type_root("fibre") == "wired"
    assert get_connectivity_type_root("copper") == "wired"


def test_get_connectivity_type_root_wireless():
    assert get_connectivity_type_root("cellular") == "wireless"
    assert get_connectivity_type_root("satellite") == "wireless"


def test_add_missing_columns(spark_session):
    schema = StructType(
        [StructField("id", IntegerType()), StructField("name", StringType())]
    )
    df = spark_session.createDataFrame([(1, "Alice")], schema)

    target_schema = [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("age", IntegerType()),
        StructField("city", StringType()),
    ]

    result = add_missing_columns(df, target_schema)
    assert "age" in result.columns
    assert "city" in result.columns
    assert result.count() == 1


def test_column_mapping_rename(spark_session):
    df = spark_session.createDataFrame([{"old_col": "value1", "keep_col": "value2"}])
    mapping = {"old_col": "new_col"}

    result, applied_mapping = column_mapping_rename(df, mapping)
    assert "new_col" in result.columns
    assert "old_col" not in result.columns
    assert "keep_col" in result.columns
    assert applied_mapping == {"old_col": "new_col"}
