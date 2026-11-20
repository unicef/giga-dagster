"""dq_report: Rejected and Warnings tables of the DQ report, configured in NocoDB."""

import os

import pandas as pd
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

from src.data_quality_checks import critical, utils  # noqa: E402
from src.data_quality_checks.utils import build_dq_report  # noqa: E402

SCHEMA = StructType(
    [
        StructField("school_id_govt", StringType(), True),
        StructField("is_new_school", BooleanType(), True),
        StructField("dq_has_critical_error", IntegerType(), True),
        StructField("dq_results", MapType(StringType(), IntegerType()), True),
    ]
)

CHECKS_COLUMNS = [
    "DQ Table Column Name",
    "DQR Row",
    "DQR Row Text",
    "DQR Group",
    "DQR Group Text",
    "DQR Priority",
    "DQR Order",
]

SCHOOLS_TABLE = pd.DataFrame(
    [
        (
            "dq_is_null_optional-latitude",
            "missing_coordinates",
            "Schools with missing coordinates",
            "coordinates",
            "Coordinates",
            "High",
            1,
        ),
        (
            "dq_is_null_optional-longitude",
            "missing_coordinates",
            None,
            None,
            None,
            None,
            1,
        ),
        (
            "dq_is_not_within_country",
            "outside_country",
            "Schools with coordinates outside the country's limits",
            "coordinates",
            None,
            "high",
            2,
        ),
        (
            "dq_duplicate_location_rows_flag",
            "same_location",
            "Schools with the same exact location",
            "duplicates",
            "Duplicates",
            "Medium",
            10,
        ),
        (
            "dq_duplicate_location_rows_id",
            "duplicate_location_groups",
            "Number of groups with {count} or more schools sharing exact location",
            "duplicates",
            None,
            None,
            11,
        ),
        (
            "dq_column_relation_checks-num_students_num_students_girls_num_students_boys",
            "students_vs_gender",
            "Schools where the number of students doesn't match the sum of girls and boys",
            "consistency",
            "Consistency",
            "Low",
            30,
        ),
        ("dq_is_null_optional-school_address", None, None, None, None, None, None),
    ],
    columns=CHECKS_COLUMNS,
)

ALWAYS = {"is_not_within_country", "duplicate-school_id_govt"}
CREATE_ONLY = {"is_null_optional-latitude", "is_null_optional-longitude"}
SUMMARY = {
    "count_duplicate_location_groups": 2,
    "duplicate_location_group_min_size": 3,
}


def _df(spark, rows: list[dict]):
    columns = [field.name for field in SCHEMA.fields]
    defaults = {
        "school_id_govt": "x",
        "is_new_school": True,
        "dq_has_critical_error": 0,
        "dq_results": {},
    }
    data = [tuple({**defaults, **row}[name] for name in columns) for row in rows]
    return spark.createDataFrame(data, schema=SCHEMA)


def _geolocation_df(spark):
    base = {
        "is_null_optional-latitude": 0,
        "is_null_optional-longitude": 0,
        "is_not_within_country": 0,
        "duplicate-school_id_govt": 0,
        "duplicate_location_rows_flag": 0,
        "is_null_optional-school_address": 0,
    }
    return _df(
        spark,
        [
            # New school missing both coordinates: one row, not two.
            {
                "dq_has_critical_error": 1,
                "dq_results": {
                    **base,
                    "is_null_optional-latitude": 1,
                    "is_null_optional-longitude": 1,
                },
            },
            # New school missing longitude only.
            {
                "dq_has_critical_error": 1,
                "dq_results": {**base, "is_null_optional-longitude": 1},
            },
            # Update missing latitude: create_only, so not rejected.
            {
                "is_new_school": False,
                "dq_results": {**base, "is_null_optional-latitude": 1},
            },
            # Rejected for outside country and duplicated location.
            {
                "dq_has_critical_error": 1,
                "dq_results": {
                    **base,
                    "is_not_within_country": 1,
                    "duplicate_location_rows_flag": 1,
                },
            },
            # Rejected for a duplicate ID, which has no configured row.
            {
                "dq_has_critical_error": 1,
                "dq_results": {**base, "duplicate-school_id_govt": 1},
            },
            # Approved, duplicated location: the only warning that counts.
            {"dq_results": {**base, "duplicate_location_rows_flag": 1}},
        ],
    )


def _report(spark, df=None, table=SCHOOLS_TABLE, summary=SUMMARY, labels=None):
    return build_dq_report(
        df if df is not None else _geolocation_df(spark),
        table,
        "schools",
        (ALWAYS, CREATE_ONLY),
        summary,
        labels or {"dq_duplicate-school_id_govt": "school_id_govt has a duplicate"},
    )


def _rows(report, outcome):
    return {
        row["key"]: {**row, "group": group["key"]}
        for group in report[outcome]
        for row in group["rows"]
    }


def test_no_nocodb_configuration_returns_none(spark):
    table = SCHOOLS_TABLE.drop(columns=["DQR Row"])
    assert _report(spark, table=table) is None


def test_envelope(spark):
    report = _report(spark)
    assert report["version"] == 1
    assert report["entity"] == "schools"


def test_rejected_counts_union_with_create_only_for_new_schools(spark):
    rejected = _rows(_report(spark), "rejected")
    missing = rejected["missing_coordinates"]
    assert missing["count"] == 2
    assert missing["checks"] == [
        "is_null_optional-latitude",
        "is_null_optional-longitude",
    ]
    assert missing["label"] == "Schools with missing coordinates"
    assert missing["priority"] == "High"
    assert missing["kind"] == "rows"
    assert rejected["outside_country"]["count"] == 1
    assert rejected["outside_country"]["priority"] == "High"


def test_warnings_count_approved_rows_only(spark):
    warnings = _rows(_report(spark), "warnings")
    assert warnings["same_location"]["count"] == 1
    assert warnings["same_location"]["priority"] == "Medium"


def test_unconfigured_critical_check_gets_fallback_row(spark):
    rejected = _rows(_report(spark), "rejected")
    fallback = rejected["duplicate-school_id_govt"]
    assert fallback["count"] == 1
    assert fallback["group"] == "other"
    assert fallback["label"] == "school_id_govt has a duplicate"
    assert fallback["priority"] == "High"


def test_group_row_uses_summary_tally(spark):
    groups = _rows(_report(spark), "warnings")["duplicate_location_groups"]
    assert groups["kind"] == "groups"
    assert groups["count"] == 2
    assert groups["params"] == {"count": 3}
    assert (
        groups["label"]
        == "Number of groups with 3 or more schools sharing exact location"
    )
    assert groups["priority"] == "Low"


def test_group_row_omitted_without_summary_tally(spark):
    report = _report(spark, summary={})
    assert "duplicate_location_groups" not in _rows(report, "warnings")


def test_rows_whose_checks_did_not_run_are_omitted(spark):
    report = _report(spark)
    assert "students_vs_gender" not in _rows(report, "warnings")


def test_checks_without_dqr_row_are_left_out(spark):
    report = _report(spark)
    all_checks = {
        check
        for outcome in ("rejected", "warnings")
        for row in _rows(report, outcome).values()
        for check in row["checks"]
    }
    assert "is_null_optional-school_address" not in all_checks


def test_groups_and_rows_follow_dqr_order(spark):
    report = _report(spark)
    assert [group["key"] for group in report["rejected"]] == ["coordinates", "other"]
    assert [row["key"] for row in report["rejected"][0]["rows"]] == [
        "missing_coordinates",
        "outside_country",
    ]
    assert report["rejected"][0]["label"] == "Coordinates"
    assert report["rejected"][1]["label"] == "Other"
    assert [row["key"] for row in report["warnings"][0]["rows"]] == [
        "same_location",
        "duplicate_location_groups",
    ]


def test_flat_dq_columns_for_coverage(spark):
    df = spark.createDataFrame(
        [
            ("a", 1, 1, 0, 0),
            ("b", 1, 0, 1, 1),
            ("c", 0, 0, 0, 0),
        ],
        schema=(
            "school_id_giga string, dq_has_critical_error int, "
            "`dq_is_null_mandatory-school_id_giga` int, "
            "`dq_duplicate-school_id_giga` int, dq_school_not_in_school_master int"
        ),
    )
    report = build_dq_report(
        df,
        SCHOOLS_TABLE,
        "schools",
        (
            {
                "is_null_mandatory-school_id_giga",
                "duplicate-school_id_giga",
                "school_not_in_school_master",
            },
            set(),
        ),
        {},
        {"dq_school_not_in_school_master": "school does not exist in school master"},
    )
    rejected = _rows(report, "rejected")
    assert {key: row["count"] for key, row in rejected.items()} == {
        "duplicate-school_id_giga": 1,
        "is_null_mandatory-school_id_giga": 1,
        "school_not_in_school_master": 1,
    }
    assert (
        rejected["school_not_in_school_master"]["label"]
        == "school does not exist in school master"
    )
    assert report["warnings"] == []


def test_critical_keys_for_coverage_come_from_critical_py():
    always, create_only = utils.get_critical_check_keys("coverage_fb")
    assert always == {
        "is_null_mandatory-school_id_giga",
        "duplicate-school_id_giga",
        "school_not_in_school_master",
    }
    assert create_only == set()


def test_critical_keys_for_geolocation_come_from_nocodb(monkeypatch):
    monkeypatch.setattr(
        utils,
        "get_critical_check_columns",
        lambda: (["dq_is_not_within_country"], ["dq_is_null_optional-latitude"]),
    )
    assert utils.get_critical_check_keys("geolocation") == (
        {"is_not_within_country"},
        {"is_null_optional-latitude"},
    )


def _full_table() -> pd.DataFrame:
    table = SCHOOLS_TABLE.copy()
    table["DQ Check Category"] = [
        "critical checks",
        "critical checks",
        "critical checks",
        "location checks",
        "location checks",
        "column relation checks",
        "missing value checks",
    ]
    table["Human Readable Name"] = table["DQ Table Column Name"]
    table["Critical For"] = [
        "create_only",
        "create_only",
        "always",
        None,
        None,
        None,
        None,
    ]
    table["DQR Warnings"] = [None, None, None, "Yes", "Yes", "Yes", None]
    table["UI Error Description"] = None
    duplicate_id = pd.DataFrame(
        [
            {
                "DQ Table Column Name": "dq_duplicate-school_id_govt",
                "DQ Check Category": "critical checks",
                "Human Readable Name": "Duplicate school ID",
                "Critical For": "always",
            }
        ]
    )
    return pd.concat([table, duplicate_id], ignore_index=True)


def _mock_nocodb(monkeypatch, table: pd.DataFrame):
    for module in (utils, critical):
        monkeypatch.setattr(module, "get_nocodb_table_id_from_name", lambda **_: "t")
        monkeypatch.setattr(
            module, "get_nocodb_table_as_pandas_dataframe", lambda **_: table.copy()
        )


def test_summary_carries_dq_report_for_schools(spark, monkeypatch):
    _mock_nocodb(monkeypatch, _full_table())
    df = _geolocation_df(spark)
    result = utils.build_dq_summary_statistics(
        spark, df, df, entity="schools", dataset_type="geolocation"
    )
    assert {"summary", "critical_checks", "location_checks", "dq_report"} <= set(result)
    rejected = _rows(result["dq_report"], "rejected")
    assert rejected["missing_coordinates"]["count"] == 2
    assert rejected["duplicate-school_id_govt"]["label"] == (
        "school_id_govt has a duplicate"
    )
    assert _rows(result["dq_report"], "warnings")["same_location"]["count"] == 1


def test_summary_without_entity_has_no_dq_report(spark, monkeypatch):
    _mock_nocodb(monkeypatch, _full_table())
    df = _geolocation_df(spark)
    result = utils.build_dq_summary_statistics(spark, df, df)
    assert "dq_report" not in result


def test_summary_survives_all_empty_nocodb_columns(spark, monkeypatch):
    table = _full_table()
    table["DQR Unused"] = None
    _mock_nocodb(monkeypatch, table)
    df = _geolocation_df(spark)
    result = utils.build_dq_summary_statistics(spark, df, df)
    assert "critical_checks" in result
