from src.data_quality_checks.standard import (
    completeness_checks,
    domain_checks,
    duplicate_checks,
    is_string_more_than_255_characters_check,
    range_checks,
)


def test_duplicate_checks(spark_session):
    df = spark_session.createDataFrame(
        [{"id": 1, "val": "a"}, {"id": 1, "val": "b"}, {"id": 2, "val": "c"}]
    )

    result = duplicate_checks(df, ["id"])

    assert "dq_duplicate-id" in result.columns

    rows = result.collect()
    rows.sort(key=lambda x: x.val)

    assert rows[0]["dq_duplicate-id"] == 1
    assert rows[1]["dq_duplicate-id"] == 1
    assert rows[2]["dq_duplicate-id"] == 0


def test_completeness_checks(spark_session):
    df = spark_session.createDataFrame(
        [
            {"mandatory": "ok", "optional": "ok", "lat": 10.0},
            {"mandatory": None, "optional": None, "lat": float("nan")},
        ]
    )

    result = completeness_checks(df, ["mandatory"])

    assert "dq_is_null_mandatory-mandatory" in result.columns
    rows = result.collect()

    assert rows[0]["dq_is_null_mandatory-mandatory"] == 0
    assert rows[1]["dq_is_null_mandatory-mandatory"] == 1

    assert "dq_is_null_optional-optional" in result.columns
    assert rows[0]["dq_is_null_optional-optional"] == 0
    assert rows[1]["dq_is_null_optional-optional"] == 1


def test_range_checks(spark_session):
    data = [{"val": 5}, {"val": -1}, {"val": 15}]
    df = spark_session.createDataFrame(data)

    config = {"val": {"min": 0, "max": 10}}
    result = range_checks(df, config)

    assert "dq_is_invalid_range-val" in result.columns
    rows = result.collect()

    assert rows[0]["dq_is_invalid_range-val"] == 0
    assert rows[1]["dq_is_invalid_range-val"] == 1
    assert rows[2]["dq_is_invalid_range-val"] == 1


def test_domain_checks(spark_session):
    data = [{"cat": "A"}, {"cat": "b"}, {"cat": "z"}]
    df = spark_session.createDataFrame(data)

    config = {"cat": ["a", "b", "c"]}
    result = domain_checks(df, config)

    assert "dq_is_invalid_domain-cat" in result.columns
    rows = result.collect()

    assert rows[0]["dq_is_invalid_domain-cat"] == 0
    assert rows[1]["dq_is_invalid_domain-cat"] == 0
    assert rows[2]["dq_is_invalid_domain-cat"] == 1


def test_format_validation_checks(spark_session):
    pass


def test_string_length_check(spark_session):
    long_str = "a" * 256
    short_str = "a" * 10

    data = [{"text": short_str}, {"text": long_str}]
    df = spark_session.createDataFrame(data)

    result = is_string_more_than_255_characters_check(df)

    assert "dq_is_string_more_than_255_characters-text" in result.columns
    rows = result.collect()

    assert rows[0]["dq_is_string_more_than_255_characters-text"] == 0
    assert rows[1]["dq_is_string_more_than_255_characters-text"] == 1
