from unittest.mock import patch

from pyspark.sql.types import StringType, StructField, StructType
from src.constants import UploadMode
from src.spark.transform_functions import (
    create_education_level,
    generate_uuid,
)


def test_generate_uuid():
    input_str = "test_string"
    uuid1 = generate_uuid(input_str)
    uuid2 = generate_uuid(input_str)
    assert uuid1 == uuid2
    assert isinstance(uuid1, str)
    assert len(uuid1) > 0


@patch("src.spark.transform_functions.get_nocodb_table_id_from_name")
@patch("src.spark.transform_functions.get_nocodb_table_as_key_value_mapping")
def test_create_education_level(mock_get_mapping, mock_get_id, spark_session):
    mock_get_id.return_value = "table_id"
    mock_get_mapping.return_value = {
        "Primary": "Primary (Standard)",
        "Secondary": "Secondary (Standard)",
    }
    data = [("Primary", None), ("Secondary", None), ("Unknown", None)]
    schema = StructType(
        [
            StructField("education_level_govt", StringType(), True),
            StructField("education_level", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)
    uploaded_columns = ["education_level_govt"]
    result_df = create_education_level(df, UploadMode.CREATE.value, uploaded_columns)
    results = {
        row["education_level_govt"]: row["education_level"]
        for row in result_df.collect()
    }
    assert results["Primary"] == "Primary (Standard)"
    assert results["Secondary"] == "Secondary (Standard)"
    assert results["Unknown"] == "Unknown"
