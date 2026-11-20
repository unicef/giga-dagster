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

RELATION = "column_relation_checks"
STUDENTS = f"{RELATION}-num_students_num_students_girls_num_students_boys"
TEACHERS = f"{RELATION}-num_teachers_num_teachers_female_num_teachers_male"

# Same configuration as SchoolGeolocationMasterDQChecks in dev/stg:
# (check, category, critical for, DQR warnings,
#  row, row text, group, group text, priority, order, optional)
NOCODB_ROWS = [
    ("is_null_optional-school_name", "critical checks", "create_only", None,
     "missing_names", "Schools with missing names", "other", "Other", "High", 50, None),
    ("is_null_mandatory-school_id_govt", "critical checks", "always", None,
     "missing_id", "Schools with missing ID", "ids", "School IDs", "High", 30, None),
    ("is_null_optional-education_level_govt", "critical checks", "create_only", None,
     "missing_education_level", "Schools with missing educational level", "other",
     None, "High", 60, None),
    ("is_null_optional-latitude", "critical checks", "create_only", None,
     "missing_coordinates", "Schools with missing coordinates", "coordinates",
     "Coordinates", "High", 10, None),
    ("is_null_optional-longitude", "critical checks", "create_only", None,
     "missing_coordinates", None, "coordinates", None, "High", 10, None),
    ("duplicate-school_id_govt", "critical checks", "always", None,
     "duplicate_ids", "Schools with duplicate school IDs", "ids", None, "High", 40,
     None),
    ("is_not_within_country", "critical checks", "always", None,
     "outside_country", "Schools with coordinates outside the country's limits",
     "coordinates", None, "High", 20, None),
    ("is_invalid_range-latitude", "critical checks", "always", None,
     None, None, None, None, None, None, None),
    ("duplicate_location_rows_flag", "location checks", None, "Yes",
     "same_location", "Schools with the same exact location", "duplicates",
     "Duplicates", "Medium", 100, None),
    ("duplicate_location_rows_id", "location checks", None, "Yes",
     "duplicate_location_groups",
     "Number of groups with {count} or more schools sharing exact location",
     "duplicates", None, None, 110, None),
    ("duplicate_set-school_name_education_level_location_id", "location checks",
     None, "Yes", "same_name_level_location",
     "Schools with the same name, educational level, and location", "duplicates",
     None, "Medium", 120, None),
    ("precision-latitude", "precision checks", None, "Yes",
     "low_precision", "Schools with low precision coordinates (less than 5 digits)",
     "accuracy", "Accuracy", "Medium", 200, None),
    ("precision-longitude", "precision checks", None, "Yes",
     "low_precision", None, "accuracy", None, "Medium", 200, None),
    ("is_in_uninhabited_area", "location checks", None, "Yes",
     "not_inhabited_area", "Schools not in an inhabited area", "accuracy", None,
     "Medium", 210, None),
    ("is_suspect_location", "location_checks", None, None,
     "not_near_built_environment", "Schools not near a built environment",
     "accuracy", None, "Low", 220, None),
    ("duplicate_group_flag_50m", "location checks", None, "Yes",
     "within_50m", "Schools within 50m of another school", "other", "Other", "Low",
     300, None),
    ("is_school_density_greater_than_5", "location checks", None, "Yes",
     "high_density", "High Density (more than 5 schools within 500m)", "other",
     None, "Low", 330, None),
    (STUDENTS, "consistency checks", None, "Yes",
     "students_vs_gender",
     "Schools where the number of students doesn't match the sum of girls and boys",
     "consistency", "Consistency", "Low", 400, "Yes"),
    (TEACHERS, "consistency checks", None, "Yes",
     "teachers_vs_gender",
     "Schools where the number of teachers doesn't match the sum of female and "
     "male teachers", "consistency", None, "Low", 410, "Yes"),
    ("is_null_optional-school_address", "missing value checks", None, None,
     None, None, None, None, None, None, None),
]  # fmt: skip

NOCODB_TABLE = pd.DataFrame(
    [
        {
            "DQ Table Column Name": f"dq_{check}",
            "DQ Check Category": category,
            "Human Readable Name": check,
            "Critical For": critical_for,
            "DQR Warnings": warning,
            "UI Error Description": None,
            "DQR Row": row,
            "DQR Row Text": text,
            "DQR Group": group,
            "DQR Group Text": group_text,
            "DQR Priority": priority,
            "DQR Order": order,
            "DQR Optional": optional,
        }
        for (
            check,
            category,
            critical_for,
            warning,
            row,
            text,
            group,
            group_text,
            priority,
            order,
            optional,
        ) in NOCODB_ROWS
    ]
)

CRITICAL = {
    "is_null_optional-school_name",
    "is_null_mandatory-school_id_govt",
    "is_null_optional-education_level_govt",
    "is_null_optional-latitude",
    "is_null_optional-longitude",
    "duplicate-school_id_govt",
    "is_not_within_country",
    "is_invalid_range-latitude",
}

BASE_RESULTS = {
    "is_null_optional-school_name": 0,
    "is_null_mandatory-school_id_govt": 0,
    "is_null_optional-latitude": 0,
    "is_null_optional-longitude": 0,
    "duplicate-school_id_govt": 0,
    "is_not_within_country": 0,
    "is_invalid_range-latitude": 0,
    "duplicate_location_rows_flag": 0,
    "duplicate_set-school_name_education_level_location_id": 0,
    "precision-latitude": 0,
    "precision-longitude": 0,
    "is_in_uninhabited_area": 0,
    "is_suspect_location": 0,
    "duplicate_group_flag_50m": 0,
    "is_school_density_greater_than_5": 0,
    STUDENTS: 0,
    "is_null_optional-school_address": 0,
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
    def failing(*keys, **row):
        return {**row, "dq_results": {**BASE_RESULTS, **dict.fromkeys(keys, 1)}}

    return _df(
        spark,
        [
            # New school missing both coordinates.
            failing(
                "is_null_optional-latitude",
                "is_null_optional-longitude",
                dq_has_critical_error=1,
            ),
            # New school missing longitude only.
            failing("is_null_optional-longitude", dq_has_critical_error=1),
            # Updates missing create_only values: approved, but in the counts.
            failing("is_null_optional-latitude", is_new_school=False),
            failing("is_null_optional-latitude", is_new_school=False),
            failing("is_null_optional-school_name", is_new_school=False),
            # Rejected, outside country and duplicated location.
            failing(
                "is_not_within_country",
                "duplicate_location_rows_flag",
                dq_has_critical_error=1,
            ),
            # Rejected for a duplicate ID and a range check without a report row.
            failing(
                "duplicate-school_id_govt",
                "is_invalid_range-latitude",
                dq_has_critical_error=1,
            ),
            # Approved warnings.
            failing("duplicate_location_rows_flag", "precision-latitude"),
            failing("precision-latitude", "precision-longitude", "is_suspect_location"),
            failing("is_in_uninhabited_area", STUDENTS),
            failing("is_null_optional-school_address"),
        ],
    )


def _coverage_df(spark):
    return spark.createDataFrame(
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


def _mock_nocodb(monkeypatch, table: pd.DataFrame):
    for module in (utils, critical):
        monkeypatch.setattr(module, "get_nocodb_table_id_from_name", lambda **_: "t")
        monkeypatch.setattr(
            module, "get_nocodb_table_as_pandas_dataframe", lambda **_: table.copy()
        )


def _summary(spark, monkeypatch, df=None, table=NOCODB_TABLE, **kwargs):
    _mock_nocodb(monkeypatch, table)
    df = df if df is not None else _geolocation_df(spark)
    return utils.build_dq_summary_statistics(spark, df, df, **kwargs)


def _rows(report, outcome):
    return {
        row["key"]: {**row, "group": group["key"]}
        for group in report[outcome]
        for row in group["rows"]
    }


def _counts(report):
    return {
        key: row["count"]
        for outcome in ("rejected", "warnings")
        for key, row in _rows(report, outcome).items()
    }


def _ui_counts(dq_summary: dict) -> dict[str, int]:
    """The counts Giga Sync computes today from the dq_summary sections
    (ui/src/constants/dqReportSections.ts, email/src/lib/pdf-generator.ts)."""

    def failed(section: str, assertion: str, column: str | None = None) -> int:
        for check in dq_summary.get(section, []):
            if check["assertion"] == assertion and (
                column is None or check["column"] == column
            ):
                return int(check["count_failed"])
        return 0

    summary = dq_summary["summary"]
    counts = {
        "missing_coordinates": max(
            failed("critical_checks", "is_null_optional", "latitude"),
            failed("critical_checks", "is_null_optional", "longitude"),
        ),
        "outside_country": failed("critical_checks", "is_not_within_country"),
        "missing_id": failed("critical_checks", "is_null_mandatory", "school_id_govt"),
        "duplicate_ids": summary["count_duplicate_school_id"],
        "missing_names": failed("critical_checks", "is_null_optional", "school_name"),
        "missing_education_level": failed(
            "critical_checks", "is_null_optional", "education_level_govt"
        ),
        "same_location": failed(
            "location_checks", "duplicate_location_rows", "location_id"
        ),
        "same_name_level_location": failed(
            "location_checks",
            "duplicate_set",
            "school_name_education_level_location_id",
        ),
        "low_precision": summary["count_schools_low_precision_coordinates"],
        "not_inhabited_area": failed("location_checks", "is_in_uninhabited_area"),
        "not_near_built_environment": failed("location_checks", "is_suspect_location"),
        "within_50m": failed("location_checks", "duplicate_group_flag_50m"),
        "high_density": failed("location_checks", "is_school_density_greater_than_5"),
    }
    # Optional rows: shown only when the check is in the report.
    consistency = dq_summary.get("consistency_checks", [])
    for key, check_key in (
        ("students_vs_gender", STUDENTS),
        ("teachers_vs_gender", TEACHERS),
    ):
        column = check_key.split("-", 1)[1]
        if any(check["column"] == column for check in consistency):
            counts[key] = failed("consistency_checks", RELATION, column)
    return counts


def test_geolocation_matches_current_giga_sync_tables(spark, monkeypatch):
    result = _summary(spark, monkeypatch, entity="schools", dataset_type="geolocation")
    assert _counts(result["dq_report"]) == _ui_counts(result)
    assert _counts(result["dq_report"]) == {
        "missing_coordinates": 3,
        "outside_country": 1,
        "missing_id": 0,
        "duplicate_ids": 1,
        "missing_names": 1,
        "missing_education_level": 0,
        "same_location": 1,
        "same_name_level_location": 0,
        "low_precision": 2,
        "not_inhabited_area": 1,
        "not_near_built_environment": 1,
        "within_50m": 0,
        "high_density": 0,
        "students_vs_gender": 1,
    }


def test_geolocation_layout(spark, monkeypatch):
    report = _summary(spark, monkeypatch, entity="schools", dataset_type="geolocation")[
        "dq_report"
    ]
    assert report["version"] == 1
    assert report["entity"] == "schools"
    assert [
        (group["key"], group["label"], [row["key"] for row in group["rows"]])
        for group in report["rejected"]
    ] == [
        ("coordinates", "Coordinates", ["missing_coordinates", "outside_country"]),
        ("ids", "School IDs", ["missing_id", "duplicate_ids"]),
        ("other", "Other", ["missing_names", "missing_education_level"]),
    ]
    assert [
        (group["key"], [row["key"] for row in group["rows"]])
        for group in report["warnings"]
    ] == [
        ("duplicates", ["same_location", "same_name_level_location"]),
        (
            "accuracy",
            ["low_precision", "not_inhabited_area", "not_near_built_environment"],
        ),
        ("other", ["within_50m", "high_density"]),
        ("consistency", ["students_vs_gender"]),
    ]
    missing = _rows(report, "rejected")["missing_coordinates"]
    assert missing["label"] == "Schools with missing coordinates"
    assert missing["priority"] == "High"
    assert missing["checks"] == [
        "is_null_optional-latitude",
        "is_null_optional-longitude",
    ]


def test_unconfigured_checks_stay_out_of_geolocation(spark, monkeypatch):
    report = _summary(spark, monkeypatch, entity="schools", dataset_type="geolocation")[
        "dq_report"
    ]
    checks = {
        check
        for outcome in ("rejected", "warnings")
        for row in _rows(report, outcome).values()
        for check in row["checks"]
    }
    assert "is_invalid_range-latitude" not in checks
    assert "is_null_optional-school_address" not in checks


def test_row_whose_check_did_not_run_shows_zero_unless_optional(spark):
    df = _df(spark, [{"dq_results": {"is_null_optional-latitude": 0}}])
    report = build_dq_report(df, NOCODB_TABLE, "schools", CRITICAL, {}, True)
    counts = _counts(report)
    assert counts["outside_country"] == 0
    assert counts["same_location"] == 0
    assert "students_vs_gender" not in counts
    assert "teachers_vs_gender" not in counts


def test_group_tally_rows(spark):
    df = _df(spark, [{"dq_results": BASE_RESULTS}])
    summary = {
        "count_duplicate_location_groups": 2,
        "duplicate_location_group_min_size": 3,
    }
    report = build_dq_report(df, NOCODB_TABLE, "schools", CRITICAL, summary, True)
    groups = _rows(report, "warnings")["duplicate_location_groups"]
    assert groups["kind"] == "groups"
    assert groups["count"] == 2
    assert groups["params"] == {"count": 3}
    assert (
        groups["label"]
        == "Number of groups with 3 or more schools sharing exact location"
    )
    report = build_dq_report(df, NOCODB_TABLE, "schools", CRITICAL, {}, True)
    assert "duplicate_location_groups" not in _rows(report, "warnings")


def test_no_nocodb_configuration_returns_none(spark):
    df = _df(spark, [{"dq_results": BASE_RESULTS}])
    table = NOCODB_TABLE.drop(columns=["DQR Row"])
    assert build_dq_report(df, table, "schools", CRITICAL, {}, True) is None


def test_coverage_gets_its_own_critical_checks(spark, monkeypatch):
    report = _summary(
        spark,
        monkeypatch,
        df=_coverage_df(spark),
        entity="schools",
        dataset_type="coverage_fb",
    )["dq_report"]
    assert _counts(report) == {
        "duplicate-school_id_giga": 1,
        "is_null_mandatory-school_id_giga": 1,
        "school_not_in_school_master": 1,
    }
    assert [group["key"] for group in report["rejected"]] == ["other"]
    assert (
        _rows(report, "rejected")["school_not_in_school_master"]["label"]
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


def test_summary_without_entity_has_no_dq_report(spark, monkeypatch):
    assert "dq_report" not in _summary(spark, monkeypatch)


def test_summary_survives_all_empty_nocodb_columns(spark, monkeypatch):
    table = NOCODB_TABLE.copy()
    table["DQR Unused"] = None
    assert "critical_checks" in _summary(spark, monkeypatch, table=table)
