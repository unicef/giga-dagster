from unittest.mock import MagicMock

from pyspark.sql import Row
from src.data_quality_checks.coverage import fb_percent_sum_to_100_check


def test_fb_percent_sum_to_100_check(spark_session):
    data = [
        Row(percent_2G=50, percent_3G=30, percent_4G=20),
        Row(percent_2G=50, percent_3G=30, percent_4G=10),
        Row(percent_2G=0, percent_3G=0, percent_4G=0),
    ]
    df = spark_session.createDataFrame(data)

    context = MagicMock()
    result_df = fb_percent_sum_to_100_check(df, context)

    rows = result_df.collect()
    assert rows[0]["dq_is_sum_of_percent_not_equal_100"] == 0
    assert rows[1]["dq_is_sum_of_percent_not_equal_100"] == 1
    assert rows[2]["dq_is_sum_of_percent_not_equal_100"] == 1
