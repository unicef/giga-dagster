import pandas as pd
from src.constants import DataTier
from src.utils.metadata import get_output_metadata, get_table_preview
from src.utils.op_config import FileConfig


def test_get_output_metadata():
    config = FileConfig(
        filepath="test/path.csv",
        destination_filepath="dest/path.csv",
        metastore_schema="test_schema",
        dataset_type="geolocation",
        country_code="BRA",
        file_size_bytes=1000,
        tier=DataTier.SILVER,
        metadata={"custom_key": "custom_value"},
    )
    metadata = get_output_metadata(config)
    assert metadata["dataset_type"] == "geolocation"
    assert metadata["country_code"] == "BRA"
    assert metadata["custom_key"] == "custom_value"
    assert metadata["tier"] == "SILVER"


def test_get_output_metadata_with_filepath():
    config = FileConfig(
        filepath="test/path.csv",
        destination_filepath="dest/path.csv",
        metastore_schema="test_schema",
        dataset_type="coverage",
        country_code="USA",
        file_size_bytes=2000,
        tier=DataTier.BRONZE,
    )
    metadata = get_output_metadata(config, filepath="custom/path.csv")
    assert metadata["filepath"] == "custom/path.csv"


def test_get_table_preview_pandas():
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    preview = get_table_preview(df, count=2)
    assert preview is not None


def test_get_table_preview_pyspark(spark_session):
    data = [(1, "a"), (2, "b"), (3, "c")]
    df = spark_session.createDataFrame(data, ["col1", "col2"])
    preview = get_table_preview(df, count=2)
    assert preview is not None


def test_get_table_preview_default_count(spark_session):
    data = [(i, f"val{i}") for i in range(10)]
    df = spark_session.createDataFrame(data, ["id", "value"])
    preview = get_table_preview(df)
    assert preview is not None
