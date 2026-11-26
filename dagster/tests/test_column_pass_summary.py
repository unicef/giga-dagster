"""column_pass_summary: per-column pass counts on a single base (all uploaded rows)."""

import os

import pandas as pd
import pytest
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# src.settings validates these at import time; tests must not need a real .env.
for _name in (
    "AZURE_SAS_TOKEN",
    "AZURE_BLOB_CONTAINER_NAME",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "SLACK_WEBHOOK",
    "SPARK_RPC_AUTHENTICATION_SECRET",
    "HIVE_METASTORE_URI",
    "EMAIL_RENDERER_BEARER_TOKEN",
    "INGESTION_POSTGRESQL_USERNAME",
    "INGESTION_POSTGRESQL_PASSWORD",
    "INGESTION_POSTGRESQL_DATABASE",
):
    os.environ.setdefault(_name, "test")

from src.data_quality_checks import critical  # noqa: E402
from src.data_quality_checks.utils import (  # noqa: E402
    _check_key_columns,
    build_column_pass_summary,
)

SCHEMA = StructType(
    [
        StructField("school_id_govt", StringType(), True),
        StructField("education_level_govt", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("num_students", StringType(), True),
        StructField("is_new_school", BooleanType(), True),
        StructField("dq_has_critical_error", IntegerType(), True),
        StructField("dq_results", MapType(StringType(), IntegerType()), True),
    ]
)


def _df(spark, rows: list[dict]):
    columns = [field.name for field in SCHEMA.fields]
    defaults = {
        "school_id_govt": "x",
        "education_level_govt": "Primary",
        "latitude": "1.0",
        "longitude": "2.0",
        "num_students": "10",
        "is_new_school": True,
        "dq_has_critical_error": 0,
        "dq_results": {},
    }
    data = [tuple({**defaults, **row}[name] for name in columns) for row in rows]
    return spark.createDataFrame(data, schema=SCHEMA)


@pytest.mark.parametrize(
    ("check_key", "expected"),
    [
        ("is_null_mandatory-school_id_govt", ("is_null_mandatory", ["school_id_govt"])),
        ("duplicate-school_id_govt", ("duplicate", ["school_id_govt"])),
        ("precision-latitude", ("precision", ["latitude"])),
        (
            "duplicate_location_rows_flag",
            ("duplicate_location_rows", ["latitude", "longitude"]),
        ),
        ("duplicate_location_rows_count", None),
        ("duplicate_group_count_50m", None),
        ("duplicate_group_flag_50m", None),
        ("is_in_uninhabited_area", None),
        ("is_suspect_location", None),
        ("column_relation_checks-num_students_num_students_girls", None),
        ("has_critical_error", None),
    ],
)
def test_check_key_columns(check_key, expected):
    assert _check_key_columns(check_key) == expected


def test_no_dq_results_column_returns_empty(spark):
    df = spark.createDataFrame([("x",)], schema="school_id_govt string")
    assert build_column_pass_summary(df) == {}


def test_counts_all_rows_and_failures_overlap_once(spark):
    df = _df(
        spark,
        [
            # Rejected: fails both school_id_govt checks, counted once.
            {
                "dq_has_critical_error": 1,
                "dq_results": {
                    "is_null_mandatory-school_id_govt": 1,
                    "duplicate-school_id_govt": 1,
                },
            },
            # Rejected: duplicate only.
            {
                "dq_has_critical_error": 1,
                "dq_results": {
                    "is_null_mandatory-school_id_govt": 0,
                    "duplicate-school_id_govt": 1,
                },
            },
            # Approved, passes everything.
            {
                "dq_results": {
                    "is_null_mandatory-school_id_govt": 0,
                    "duplicate-school_id_govt": 0,
                    "is_not_numeric-num_students": 0,
                },
            },
            # Approved with a non-critical failure.
            {
                "dq_results": {
                    "is_null_mandatory-school_id_govt": 0,
                    "duplicate-school_id_govt": 0,
                    "is_not_numeric-num_students": 1,
                },
            },
        ],
    )

    summary = build_column_pass_summary(df)

    assert summary["school_id_govt"] == {
        "total": 4,
        "passed": 2,
        "failed": 2,
        "checks": {"duplicate": 2, "is_null_mandatory": 1},
    }
    # Non-critical columns share the same base (all rows), not approved only.
    assert summary["num_students"] == {
        "total": 4,
        "passed": 3,
        "failed": 1,
        "checks": {"is_not_numeric": 1},
    }


def test_missing_or_null_key_counts_as_passed(spark):
    df = _df(
        spark,
        [
            {"dq_results": {"is_null_optional-num_students": None}},
            {"dq_results": {}},
            {"dq_results": {"is_null_optional-num_students": 1}},
        ],
    )

    summary = build_column_pass_summary(df)

    assert summary["num_students"]["passed"] == 2
    assert summary["num_students"]["failed"] == 1


def test_excluded_keys_and_location_duplicate(spark):
    df = _df(
        spark,
        [
            {
                "dq_results": {
                    "duplicate_location_rows_flag": 1,
                    "duplicate_location_rows_count": 3,
                    "duplicate_group_flag_50m": 1,
                    "is_in_uninhabited_area": 1,
                    "column_relation_checks-num_students_num_students_girls": 1,
                    # Column not in the uploaded frame: left out.
                    "is_null_optional-num_computers": 1,
                    "precision-latitude": 0,
                },
            },
            {"dq_results": {"duplicate_location_rows_flag": 0}},
        ],
    )

    summary = build_column_pass_summary(df)

    assert set(summary) == {"latitude", "longitude"}
    assert summary["latitude"] == {
        "total": 2,
        "passed": 1,
        "failed": 1,
        "checks": {"duplicate_location_rows": 1, "precision": 0},
    }
    assert summary["longitude"] == {
        "total": 2,
        "passed": 1,
        "failed": 1,
        "checks": {"duplicate_location_rows": 1},
    }


def test_create_only_checks_fail_only_new_schools(spark):
    key = "is_null_mandatory-education_level_govt"
    df = _df(
        spark,
        [
            {"is_new_school": True, "dq_results": {key: 1}},
            {"is_new_school": False, "dq_results": {key: 1}},
            {"is_new_school": None, "dq_results": {key: 1}},
            {"is_new_school": False, "dq_results": {key: 0}},
        ],
    )

    summary = build_column_pass_summary(df, create_only_keys={key})
    raw = build_column_pass_summary(df)

    assert summary["education_level_govt"]["failed"] == 1
    assert summary["education_level_govt"]["passed"] == 3
    assert summary["education_level_govt"]["checks"] == {"is_null_mandatory": 1}
    assert raw["education_level_govt"]["failed"] == 3


def test_get_critical_check_columns(monkeypatch):
    table = pd.DataFrame(
        {
            "DQ Table Column Name": [
                "dq_is_null_mandatory-education_level_govt",
                "dq_is_null_mandatory-school_id_govt",
                "dq_is_not_numeric-num_students",
                "dq_duplicate-school_id_govt",
            ],
            "Critical For": ["create_only", "always", None, "always"],
        }
    )
    monkeypatch.setattr(critical, "get_nocodb_table_id_from_name", lambda **_: "id")
    monkeypatch.setattr(
        critical, "get_nocodb_table_as_pandas_dataframe", lambda **_: table
    )

    assert critical.get_critical_check_columns() == (
        ["dq_is_null_mandatory-school_id_govt", "dq_duplicate-school_id_govt"],
        ["dq_is_null_mandatory-education_level_govt"],
    )
