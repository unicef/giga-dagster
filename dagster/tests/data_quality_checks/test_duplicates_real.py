from pyspark.sql import Row
from src.data_quality_checks.duplicates import (
    duplicate_all_except_checks,
    duplicate_set_checks,
)


def test_duplicate_set_checks(spark_session):
    data = [
        Row(col1="a", col2="b", latitude=1.0, longitude=2.0),
        Row(col1="a", col2="b", latitude=1.0, longitude=2.0),
        Row(col1="a", col2="c", latitude=1.0, longitude=2.0),
    ]
    df = spark_session.createDataFrame(data)
    config_set = ["col1", "col2"]
    res = duplicate_set_checks(df, config_set)
    rows = res.sort("col2").collect()
    # The code creates separate DQ columns for each column in the set
    # "_".join("col1") creates "c_o_l_1" (joins characters)
    assert rows[0]["dq_duplicate_set-c_o_l_1"] == 1
    assert rows[0]["dq_duplicate_set-c_o_l_2"] == 1


def test_duplicate_all_except_checks(spark_session):
    data = [Row(k="k1", ign="i1"), Row(k="k1", ign="i2"), Row(k="k2", ign="i1")]
    df = spark_session.createDataFrame(data)
    res = duplicate_all_except_checks(df, ["k"])
    rows = res.sort("k").collect()
    assert rows[0]["dq_duplicate_all_except_school_code"] == 1
    assert rows[2]["dq_duplicate_all_except_school_code"] == 0
