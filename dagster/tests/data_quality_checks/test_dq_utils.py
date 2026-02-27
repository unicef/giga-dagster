from unittest.mock import patch

from src.data_quality_checks.utils import (
    aggregate_report_json,
    aggregate_report_spark_df,
    dq_split_failed_rows,
    dq_split_passed_rows,
    extract_school_id_govt_duplicates,
)


def test_extract_school_id_govt_duplicates(spark_session):
    data = [
        {"school_id_govt": "A", "val": 1},
        {"school_id_govt": "A", "val": 2},
        {"school_id_govt": "B", "val": 3},
    ]
    df = spark_session.createDataFrame(data)

    result = extract_school_id_govt_duplicates(df)

    assert "row_num" in result.columns
    rows = result.filter("school_id_govt='A'").collect()
    row_nums = sorted([r.row_num for r in rows])
    assert row_nums == [1, 2]


def test_dq_split_rows(spark_session):
    data = [
        {"school_id": 1, "dq_has_critical_error": 0},
        {"school_id": 2, "dq_has_critical_error": 1},
    ]
    df = spark_session.createDataFrame(data)

    passed = dq_split_passed_rows(df, "generic_test")
    failed = dq_split_failed_rows(df, "generic_test")

    assert passed.count() == 1
    assert failed.count() == 1
    assert passed.collect()[0]["school_id"] == 1
    assert failed.collect()[0]["school_id"] == 2


@patch("src.data_quality_checks.utils.get_nocodb_table_id_from_name")
@patch("src.data_quality_checks.utils.get_nocodb_table_as_pandas_dataframe")
def test_aggregate_report_spark_df(mock_get_df, mock_get_id, spark_session):
    import pandas as pd

    mock_get_id.return_value = "table_123"

    meta_data = pd.DataFrame(
        {
            "DQ Table Column Name": ["dq_test-col"],
            "DQ Check Category": ["validity"],
            "Human Readable Name": ["Test Validity Check"],
        }
    )
    mock_get_df.return_value = meta_data

    data = [
        {"id": 1, "dq_test-col": 1},
        {"id": 2, "dq_test-col": 0},
        {"id": 3, "dq_test-col": 0},
    ]
    df = spark_session.createDataFrame(data)

    report = aggregate_report_spark_df(spark_session, df)

    rows = report.collect()
    assert len(rows) == 1
    row = rows[0]

    assert row["column"] == "col"
    assert row["description"] == "Test Validity Check"
    assert row["count_failed"] == 1
    assert row["count_passed"] == 2
    assert row["count_overall"] == 3
    assert abs(row["percent_failed"] - 33.33) < 0.1


def test_aggregate_report_json(spark_session):
    qa_data = [
        {
            "type": "validity",
            "assertion": "test",
            "count_failed": 1,
            "dq_has_critical_error": 0,
            "column": "c1",
            "description": "desc",
            "count_passed": 1,
            "count_overall": 2,
            "percent_failed": 50.0,
            "percent_passed": 50.0,
            "dq_remarks": "fail",
        },
        {
            "type": "critical checks",
            "assertion": "crit",
            "count_failed": 0,
            "dq_has_critical_error": 1,
            "column": "c2",
            "description": "crit desc",
            "count_passed": 2,
            "count_overall": 2,
            "percent_failed": 0.0,
            "percent_passed": 100.0,
            "dq_remarks": "pass",
        },
    ]
    df_agg = spark_session.createDataFrame(qa_data)

    df_bronze = spark_session.createDataFrame([{"c1": 1, "c2": 2}])

    dq_checks_data = [
        {"dq_has_critical_error": 0},
        {"dq_has_critical_error": 1},
        {"dq_has_critical_error": 0},
    ]
    df_dq = spark_session.createDataFrame(dq_checks_data)

    result = aggregate_report_json(df_agg, df_bronze, df_dq)

    assert "summary" in result
    assert result["summary"]["rows"] == 3
    assert result["summary"]["rows_failed"] == 1

    assert "critical_checks" in result
    assert len(result["critical_checks"]) == 1
    assert result["critical_checks"][0]["column"] == "c2"

    assert "validity" in result
    assert len(result["validity"]) == 1
