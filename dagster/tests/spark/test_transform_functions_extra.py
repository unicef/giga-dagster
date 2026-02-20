from unittest.mock import patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType
from src.spark.transform_functions import (
    create_education_level,
    create_school_id_giga,
)


def test_create_school_id_giga(spark_session):
    data = [("UUID1", "CODE1"), ("UUID2", None)]
    schema = StructType(
        [
            StructField("school_id_giga", StringType(), True),
            StructField("code", StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema)

    res = create_school_id_giga(df)
    assert res.count() == 2
    assert "school_id_giga" in res.columns


@pytest.fixture
def mock_nocodb():
    with (
        patch("src.spark.transform_functions.get_nocodb_table_id_from_name") as m_id,
        patch(
            "src.spark.transform_functions.get_nocodb_table_as_key_value_mapping"
        ) as m_map,
    ):
        m_id.return_value = "table_id"
        m_map.return_value = {"Primary": "Primary"}
        yield m_map


def test_create_education_level(spark_session, mock_nocodb):
    data = [("Primary", "Primary"), ("Secondary", "Secondary"), (None, None)]
    schema = ["education_level", "education_level_govt"]
    df = spark_session.createDataFrame(data, schema)

    with patch("src.spark.transform_functions.UploadMode") as MockEnum:
        MockEnum.CREATE.value = "create"
        res = create_education_level(
            df, mode="create", uploaded_columns=["education_level"]
        )

    assert "education_level" in res.columns
    assert res.count() == 3
