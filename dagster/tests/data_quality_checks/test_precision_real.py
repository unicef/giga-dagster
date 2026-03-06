from pyspark.sql import Row
from src.data_quality_checks.precision import precision_check


def test_precision_check(spark_session):
    data = [Row(latitude=10.123, longitude=20.12), Row(latitude=10.1, longitude=20.1)]
    df = spark_session.createDataFrame(data)
    config = {"latitude": {"min": 2}, "longitude": {"min": 2}}
    res = precision_check(df, config)
    assert "dq_precision-latitude" in res.columns
    assert "dq_precision-longitude" in res.columns
