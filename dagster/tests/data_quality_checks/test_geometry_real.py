from unittest.mock import patch

from pyspark.sql import Row
from pyspark.sql.functions import lit
from src.data_quality_checks.geometry import (
    duplicate_name_level_110_check,
    school_density_check,
    similar_name_level_within_110_check,
)


def test_duplicate_name_level_110_check(spark_session):
    data = [
        Row(
            school_name="A",
            education_level="Primary",
            latitude=10.1234,
            longitude=20.1234,
        ),
        Row(
            school_name="A",
            education_level="Primary",
            latitude=10.1239,
            longitude=20.1239,
        ),
        Row(
            school_name="B",
            education_level="Primary",
            latitude=10.1234,
            longitude=20.1234,
        ),
    ]
    df = spark_session.createDataFrame(data)
    res = duplicate_name_level_110_check(df)
    rows = res.sort("school_name").collect()
    assert rows[0]["dq_duplicate_name_level_within_110m_radius"] == 1
    assert rows[1]["dq_duplicate_name_level_within_110m_radius"] == 1
    assert rows[2]["dq_duplicate_name_level_within_110m_radius"] == 0


def test_school_density_check(spark_session):
    def mock_h3(lat, lon):
        return lit("hex_A")

    with patch(
        "src.data_quality_checks.geometry.h3_geo_to_h3_udf", side_effect=mock_h3
    ):
        data = [
            Row(school_id_giga=f"{i}", latitude=10.0, longitude=10.0) for i in range(6)
        ]
        df = spark_session.createDataFrame(data)
        res = school_density_check(df)
        rows = res.collect()
        for row in rows:
            assert row["dq_is_school_density_greater_than_5"] == 1


def test_similar_name_level_within_110_check(spark_session):
    data = [
        Row(
            school_name="School Alpha",
            education_level="Primary",
            latitude=10.0,
            longitude=20.0,
        ),
        Row(
            school_name="School Alfa",
            education_level="Primary",
            latitude=10.0,
            longitude=20.0,
        ),
        Row(
            school_name="School Beta",
            education_level="Primary",
            latitude=10.0,
            longitude=20.0,
        ),
    ]

    df = spark_session.createDataFrame(data)

    with patch(
        "src.data_quality_checks.geometry.find_similar_names_in_group_udf"
    ) as mock_udf:

        def mock_udf_impl(col):
            return lit(["School Alpha", "School Alfa"])

        mock_udf.side_effect = mock_udf_impl

        res = similar_name_level_within_110_check(df)
        rows = res.sort("school_name").collect()

        alpha = next(r for r in rows if r["school_name"] == "School Alpha")
        alfa = next(r for r in rows if r["school_name"] == "School Alfa")
        beta = next(r for r in rows if r["school_name"] == "School Beta")

        assert alpha["dq_duplicate_similar_name_same_level_within_110m_radius"] == 1
        assert alfa["dq_duplicate_similar_name_same_level_within_110m_radius"] == 1
        assert beta["dq_duplicate_similar_name_same_level_within_110m_radius"] == 0
