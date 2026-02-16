from unittest.mock import patch

import pandas as pd
from pyspark.sql import Row
from src.data_quality_checks.utils import (
    aggregate_report_spark_df,
)


def test_aggregate_report_spark_df(spark_session):
    data = [Row(dq_check1=1, dq_check2=0), Row(dq_check1=0, dq_check2=0)]
    df = spark_session.createDataFrame(data)
    with (
        patch("src.data_quality_checks.utils.get_nocodb_table_id_from_name") as mock_id,
        patch(
            "src.data_quality_checks.utils.get_nocodb_table_as_pandas_dataframe"
        ) as mock_df,
    ):
        mock_id.return_value = "id"
        mock_df.return_value = pd.DataFrame(
            [
                {
                    "DQ Table Column Name": "dq_check1",
                    "DQ Check Category": "Cat1",
                    "Human Readable Name": "Desc1",
                    "Related Check ID": 1,
                },
                {
                    "DQ Table Column Name": "dq_check2",
                    "DQ Check Category": "Cat2",
                    "Human Readable Name": "Desc2",
                    "Related Check ID": 2,
                },
            ]
        )
        report = aggregate_report_spark_df(spark_session, df)
        rows = report.sort("assertion").collect()
        assert rows[0]["assertion"] == "check1"
        assert rows[0]["count_failed"] == 1
        assert rows[0]["count_passed"] == 1
