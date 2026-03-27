from unittest.mock import patch

from pyspark.sql import Row
from src.data_quality_checks.standard import (
    completeness_checks,
    domain_checks,
    duplicate_checks,
    format_validation_checks,
    range_checks,
)


def test_duplicate_checks(spark_session):
    data = [Row(id="1", val="a"), Row(id="1", val="b"), Row(id="2", val="c")]
    df = spark_session.createDataFrame(data)
    res = duplicate_checks(df, ["id"])
    rows = res.collect()
    rows.sort(key=lambda r: r.val)
    assert rows[0]["dq_duplicate-id"] == 1
    assert rows[1]["dq_duplicate-id"] == 1
    assert rows[2]["dq_duplicate-id"] == 0


def test_completeness_checks(spark_session):
    data = [
        Row(id="1", optional=None, mandatory="m"),
        Row(id="2", optional="o", mandatory=None),
    ]
    df = spark_session.createDataFrame(data)
    res = completeness_checks(df, ["mandatory"])
    rows = res.sort("id").collect()
    assert rows[0]["dq_is_null_optional-optional"] == 1
    assert rows[0]["dq_is_null_mandatory-mandatory"] == 0
    assert rows[1]["dq_is_null_optional-optional"] == 0
    assert rows[1]["dq_is_null_mandatory-mandatory"] == 1


def test_range_checks(spark_session):
    data = [Row(val=5), Row(val=15)]
    df = spark_session.createDataFrame(data)
    config_ranges = {"val": {"min": 0, "max": 10}}
    res = range_checks(df, config_ranges)
    rows = res.sort("val").collect()
    assert rows[0]["dq_is_invalid_range-val"] == 0
    assert rows[1]["dq_is_invalid_range-val"] == 1


def test_domain_checks(spark_session):
    data = [Row(cat="A"), Row(cat="Z")]
    df = spark_session.createDataFrame(data)
    config_domain = {"cat": ["a", "b"]}
    res = domain_checks(df, config_domain)
    rows = res.sort("cat").collect()
    assert rows[0]["dq_is_invalid_domain-cat"] == 0
    assert rows[1]["dq_is_invalid_domain-cat"] == 1


def test_format_validation_checks(spark_session):
    data = [
        Row(num_str="123", alpha="abc", bad_num="abc"),
        Row(num_str="12.34", alpha="123", bad_num="123"),
    ]
    df = spark_session.createDataFrame(data)
    with patch(
        "src.data_quality_checks.standard.config.DATA_TYPES",
        [("num_str", "INT"), ("alpha", "STRING")],
    ):
        res = format_validation_checks(df)
        rows = res.collect()
        assert rows[0]["dq_is_not_numeric-num_str"] == 0
        assert rows[0]["dq_is_not_alphanumeric-alpha"] == 0
