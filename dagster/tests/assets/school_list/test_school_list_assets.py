"""BDD-style functional tests for the school_list asset pipeline.

These tests exercise real Spark transformations and DQ checks -
they do NOT mock away the business logic. Only external I/O
(NocoDB, ADLS, Delta, DataHub) is mocked.

Pattern: GIVEN input data → WHEN transform/check runs → THEN assert business rules.
"""

import uuid
from unittest.mock import patch

import pytest
from pyspark.sql import functions as f
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from src.data_quality_checks.critical import critical_error_checks
from src.data_quality_checks.precision import precision_check
from src.data_quality_checks.standard import (
    completeness_checks,
    duplicate_checks,
    range_checks,
)
from src.data_quality_checks.utils import (
    dq_split_failed_rows,
    dq_split_passed_rows,
    row_level_checks,
)
from src.spark.config_expectations import config
from src.spark.transform_functions import (
    column_mapping_rename,
    create_bronze_layer_columns,
    create_school_id_giga,
    generate_uuid,
)

# ════════════════════════════════════════════════════════════════════════
#  FIXTURES
# ════════════════════════════════════════════════════════════════════════


@pytest.fixture
def bronze_schema():
    return StructType(
        [
            StructField("school_id_govt", StringType(), True),
            StructField("school_name", StringType(), True),
            StructField("education_level", StringType(), True),
            StructField("education_level_govt", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )


@pytest.fixture
def silver_schema(bronze_schema):
    """Silver has the same columns plus signature."""
    return StructType(
        bronze_schema.fields
        + [
            StructField("school_id_giga", StringType(), True),
            StructField("signature", StringType(), True),
        ]
    )


# ════════════════════════════════════════════════════════════════════════
#  1. column_mapping_rename — raw header names → schema names
# ════════════════════════════════════════════════════════════════════════


class TestColumnMappingRename:
    """GIVEN raw CSV with arbitrary headers,
    WHEN column_mapping_rename is called with a mapping,
    THEN the DataFrame columns match the schema names."""

    def test_renames_columns_per_mapping(self, spark_session):
        # GIVEN
        raw_df = spark_session.createDataFrame(
            [("G01", "School A", "10.0")],
            ["school_id", "name", "lat"],
        )
        mapping = {
            "school_id": "school_id_govt",
            "name": "school_name",
            "lat": "latitude",
        }

        # WHEN
        result_df, filtered_mapping = column_mapping_rename(raw_df, mapping)

        # THEN
        assert "school_id_govt" in result_df.columns
        assert "school_name" in result_df.columns
        assert "latitude" in result_df.columns
        assert "school_id" not in result_df.columns
        assert filtered_mapping == mapping

    def test_strips_whitespace_from_keys(self, spark_session):
        # GIVEN — keys with trailing spaces; column name also has trailing space
        raw_df = spark_session.createDataFrame([("G01",)], ["id "])
        mapping = {"id ": "school_id_govt"}

        # WHEN — the key "id " is stripped to "id", which does NOT match column "id "
        result_df, filtered = column_mapping_rename(raw_df, mapping)

        # THEN — key was stripped so the mapping becomes {"id": "school_id_govt"}
        # which doesn't match the actual column "id ", so no rename happens.
        # This documents the current behavior: stripping applies to dict keys only.
        assert "id" in filtered  # key was stripped
        assert "school_id_govt" not in result_df.columns  # rename didn't match

    def test_skips_none_keys_and_values(self, spark_session):
        # GIVEN — mapping with None entries
        raw_df = spark_session.createDataFrame([("G01", "A")], ["id", "name"])
        mapping = {"id": "school_id_govt", None: "orphan", "name": None}

        # WHEN
        result_df, filtered = column_mapping_rename(raw_df, mapping)

        # THEN — only the valid mapping is applied
        assert "school_id_govt" in result_df.columns
        assert len(filtered) == 1


# ════════════════════════════════════════════════════════════════════════
#  2. create_school_id_giga — UUID generation
# ════════════════════════════════════════════════════════════════════════


class TestCreateSchoolIdGiga:
    """GIVEN a bronze DataFrame with the 5 prerequisite columns,
    WHEN create_school_id_giga is called,
    THEN school_id_giga is a deterministic UUID-3 string."""

    def test_generates_uuid_when_all_prereqs_present(self, spark_session):
        # GIVEN
        df = spark_session.createDataFrame(
            [("G01", "School A", "Primary", 10.12345, 20.12345)],
            [
                "school_id_govt",
                "school_name",
                "education_level",
                "latitude",
                "longitude",
            ],
        )

        # WHEN
        result = create_school_id_giga(df)

        # THEN
        row = result.collect()[0]
        giga_id = row["school_id_giga"]
        assert giga_id is not None
        assert len(giga_id) == 36  # UUID format

        # Verify determinism: same inputs → same UUID
        expected = generate_uuid("G01School APrimary10.1234520.12345")
        assert giga_id == expected

    def test_returns_null_when_prereq_missing(self, spark_session):
        # GIVEN — latitude is null
        schema = StructType(
            [
                StructField("school_id_govt", StringType()),
                StructField("school_name", StringType()),
                StructField("education_level", StringType()),
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType()),
            ]
        )
        df = spark_session.createDataFrame(
            [("G01", "School A", "Primary", None, 20.0)],
            schema,
        )

        # WHEN
        result = create_school_id_giga(df)

        # THEN
        row = result.collect()[0]
        assert row["school_id_giga"] is None

    def test_preserves_existing_school_id_giga(self, spark_session):
        # GIVEN — school_id_giga already provided
        existing_uuid = str(uuid.uuid4())
        df = spark_session.createDataFrame(
            [(existing_uuid, "G01", "School A", "Primary", 10.0, 20.0)],
            [
                "school_id_giga",
                "school_id_govt",
                "school_name",
                "education_level",
                "latitude",
                "longitude",
            ],
        )

        # WHEN
        result = create_school_id_giga(df)

        # THEN — uses the provided value, not a generated one
        row = result.collect()[0]
        assert row["school_id_giga"] == existing_uuid


# ════════════════════════════════════════════════════════════════════════
#  3. create_bronze_layer_columns — silver join + education level mapping
# ════════════════════════════════════════════════════════════════════════


class TestCreateBronzeLayerColumns:
    """GIVEN uploaded data and a silver reference table,
    WHEN create_bronze_layer_columns runs,
    THEN education_level is mapped and school_id_giga is created."""

    def test_education_level_govt_maps_to_education_level(self, spark_session):
        """GIVEN education_level_govt='primary',
        WHEN NocoDB mapping says primary→Primary,
        THEN education_level should be 'Primary'."""
        df = spark_session.createDataFrame(
            [("G01", "School A", "primary", 10.12345, 20.12345)],
            [
                "school_id_govt",
                "school_name",
                "education_level_govt",
                "latitude",
                "longitude",
            ],
        )
        silver = spark_session.createDataFrame(
            [],
            StructType(
                [
                    StructField(c, StringType())
                    for c in ["school_id_govt", "school_name"]
                ]
                + [
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType()),
                ]
            ),
        )

        with (
            patch(
                "src.spark.transform_functions.get_nocodb_table_id_from_name",
                return_value="mock_table_id",
            ),
            patch(
                "src.spark.transform_functions.get_nocodb_table_as_key_value_mapping",
                return_value={
                    "primary": "Primary",
                    "secondary": "Secondary",
                    "tertiary": "Post-Secondary",
                },
            ),
            patch("src.spark.transform_functions.settings.DEPLOY_ENV", "local"),
            patch(
                "src.spark.transform_functions.get_admin_boundaries", return_value=None
            ),
            patch(
                "src.spark.transform_functions.add_disputed_region_column",
                side_effect=lambda df: df.withColumn("disputed_region", f.lit(None)),
            ),
        ):
            result = create_bronze_layer_columns(
                df,
                silver,
                country_code_iso3="BRA",
                mode="Create",
                uploaded_columns=[
                    "school_id_govt",
                    "school_name",
                    "education_level_govt",
                    "latitude",
                    "longitude",
                ],
                is_qos=True,
            )

        row = result.collect()[0]
        assert row["education_level"] == "Primary"
        assert row["school_id_giga"] is not None
        assert len(str(row["school_id_giga"])) == 36

    def test_coalesces_values_from_silver(self, spark_session):
        """GIVEN a bronze row that is missing school_name,
        WHEN silver has school_name for the same school_id_govt,
        THEN the bronze output should use the silver value."""
        schema = StructType(
            [
                StructField("school_id_govt", StringType()),
                StructField("school_name", StringType()),
            ]
        )
        df = spark_session.createDataFrame([("G01", None)], schema)
        silver = spark_session.createDataFrame(
            [("G01", "Silver School Name")], ["school_id_govt", "school_name"]
        )

        with (
            patch("src.spark.transform_functions.settings.DEPLOY_ENV", "local"),
        ):
            result = create_bronze_layer_columns(
                df,
                silver,
                country_code_iso3="BRA",
                mode="Update",
                uploaded_columns=["school_id_govt", "school_name"],
                is_qos=True,
            )

        row = result.collect()[0]
        assert row["school_name"] == "Silver School Name"


# ════════════════════════════════════════════════════════════════════════
#  4. precision_check — lat/lon decimal precision ≥ 5
# ════════════════════════════════════════════════════════════════════════


class TestPrecisionCheck:
    """GIVEN coordinates with varying decimal precision,
    WHEN precision_check runs,
    THEN rows with <5 decimal places are flagged."""

    def test_good_precision_passes(self, spark_session):
        # GIVEN — 5 decimal places
        df = spark_session.createDataFrame(
            [(10.12345, 20.12345)], ["latitude", "longitude"]
        )

        # WHEN
        result = precision_check(df, config.PRECISION)
        row = result.collect()[0]

        # THEN — 0 means "no error" (UDF returns string)
        assert int(row["dq_precision-latitude"]) == 0
        assert int(row["dq_precision-longitude"]) == 0

    def test_bad_precision_fails(self, spark_session):
        # GIVEN — only 2 decimal places
        df = spark_session.createDataFrame([(10.12, 20.12)], ["latitude", "longitude"])

        # WHEN
        result = precision_check(df, config.PRECISION)
        row = result.collect()[0]

        # THEN — 1 means "error" (UDF returns string)
        assert int(row["dq_precision-latitude"]) == 1
        assert int(row["dq_precision-longitude"]) == 1

    def test_mixed_precision(self, spark_session):
        # GIVEN — lat has 6 decimals (good), lon has 3 (bad)
        df = spark_session.createDataFrame(
            [(10.123456, 20.123)], ["latitude", "longitude"]
        )

        # WHEN
        result = precision_check(df, config.PRECISION)
        row = result.collect()[0]

        # THEN
        assert int(row["dq_precision-latitude"]) == 0
        assert int(row["dq_precision-longitude"]) == 1


# ════════════════════════════════════════════════════════════════════════
#  5. duplicate_checks — school_id_govt uniqueness
# ════════════════════════════════════════════════════════════════════════


class TestDuplicateChecks:
    """GIVEN records with duplicate school_id_govt,
    WHEN duplicate_checks runs,
    THEN duplicate rows are flagged."""

    def test_flags_duplicate_school_ids(self, spark_session):
        df = spark_session.createDataFrame(
            [("G01",), ("G01",), ("G02",)],
            ["school_id_govt"],
        )

        result = duplicate_checks(df, ["school_id_govt"])

        rows = {
            r["school_id_govt"]: r["dq_duplicate-school_id_govt"]
            for r in result.collect()
        }
        assert rows["G01"] == 1
        assert rows["G02"] == 0


# ════════════════════════════════════════════════════════════════════════
#  6. completeness_checks — mandatory vs optional null detection
# ════════════════════════════════════════════════════════════════════════


class TestCompletenessChecks:
    """GIVEN a mandatory column school_id_govt,
    WHEN a row has a null value,
    THEN dq_is_null_mandatory is 1."""

    def test_flags_null_mandatory_field(self, spark_session):
        df = spark_session.createDataFrame(
            [(None, "School A"), ("G02", "School B")],
            ["school_id_govt", "school_name"],
        )

        result = completeness_checks(df, ["school_id_govt"])
        rows = result.collect()

        null_row = [r for r in rows if r["school_name"] == "School A"][0]
        valid_row = [r for r in rows if r["school_name"] == "School B"][0]

        assert null_row["dq_is_null_mandatory-school_id_govt"] == 1
        assert valid_row["dq_is_null_mandatory-school_id_govt"] == 0

    def test_flags_null_optional_field(self, spark_session):
        schema = StructType(
            [
                StructField("school_id_govt", StringType()),
                StructField("school_name", StringType()),
            ]
        )
        df = spark_session.createDataFrame([("G01", None)], schema)

        result = completeness_checks(df, ["school_id_govt"])
        row = result.collect()[0]

        assert row["dq_is_null_optional-school_name"] == 1


# ════════════════════════════════════════════════════════════════════════
#  7. range_checks — lat/lon within valid ranges
# ════════════════════════════════════════════════════════════════════════


class TestRangeChecks:
    """GIVEN lat/lon values,
    WHEN range_checks runs with allowed [-90,90] / [-180,180],
    THEN out-of-range values are flagged."""

    def test_valid_coordinates_pass(self, spark_session):
        df = spark_session.createDataFrame([(10.0, 20.0)], ["latitude", "longitude"])

        result = range_checks(
            df,
            {
                "latitude": {"min": -90, "max": 90},
                "longitude": {"min": -180, "max": 180},
            },
        )
        row = result.collect()[0]

        assert row["dq_is_invalid_range-latitude"] == 0
        assert row["dq_is_invalid_range-longitude"] == 0

    def test_out_of_range_latitude_fails(self, spark_session):
        df = spark_session.createDataFrame([(999.0, 20.0)], ["latitude", "longitude"])

        result = range_checks(
            df,
            {
                "latitude": {"min": -90, "max": 90},
                "longitude": {"min": -180, "max": 180},
            },
        )
        row = result.collect()[0]

        assert row["dq_is_invalid_range-latitude"] == 1
        assert row["dq_is_invalid_range-longitude"] == 0


# ════════════════════════════════════════════════════════════════════════
#  8. critical_error_checks — composite critical flag
# ════════════════════════════════════════════════════════════════════════


class TestCriticalErrorChecks:
    """GIVEN a DataFrame with individual DQ check columns,
    WHEN critical_error_checks runs for 'geolocation',
    THEN dq_has_critical_error is 1 if any critical check fails."""

    def test_flags_critical_when_mandatory_null(self, spark_session):
        # GIVEN — mandatory school_id_govt is null
        df = spark_session.createDataFrame(
            [("G01", "GIGA01", 0, 0, 0, 0, 0, 0), (None, "GIGA02", 1, 0, 0, 0, 0, 0)],
            [
                "school_id_govt",
                "school_id_giga",
                "dq_is_null_mandatory-school_id_govt",
                "dq_duplicate-school_id_govt",
                "dq_duplicate-school_id_giga",
                "dq_is_null_optional-latitude",
                "dq_is_null_optional-longitude",
                "dq_is_invalid_range-latitude",
            ],
        )
        # Add remaining expected columns
        df = df.withColumn("dq_is_invalid_range-longitude", f.lit(0))
        df = df.withColumn("dq_is_not_within_country", f.lit(0))
        df = df.withColumn("dq_is_not_create", f.lit(0))

        with patch(
            "src.data_quality_checks.critical.handle_rename_dq_has_critical_error_column",
            return_value={},
        ):
            result = critical_error_checks(
                df, "geolocation", ["school_id_govt"], mode="Create"
            )

        rows = result.collect()
        # The row with null school_id_govt should be critical
        critical_row = [r for r in rows if r["school_id_giga"] == "GIGA02"][0]
        good_row = [r for r in rows if r["school_id_giga"] == "GIGA01"][0]

        assert critical_row["dq_has_critical_error"] == 1
        assert good_row["dq_has_critical_error"] == 0


# ════════════════════════════════════════════════════════════════════════
#  9. dq_split — passed / failed row filtering
# ════════════════════════════════════════════════════════════════════════


class TestDqSplit:
    """GIVEN DQ results with dq_has_critical_error 0 or 1,
    WHEN dq_split_passed/failed runs,
    THEN only the correct rows survive."""

    def test_passed_rows_excludes_critical_errors(self, spark_session):
        df = spark_session.createDataFrame(
            [("G01", 0, "no reason"), ("G02", 1, "bad data")],
            ["school_id_govt", "dq_has_critical_error", "failure_reason"],
        )

        passed = dq_split_passed_rows(df, "geolocation")
        assert passed.count() == 1
        assert passed.collect()[0]["school_id_govt"] == "G01"
        # DQ columns should be stripped
        assert "dq_has_critical_error" not in passed.columns
        assert "failure_reason" not in passed.columns

    def test_failed_rows_only_contains_errors(self, spark_session):
        df = spark_session.createDataFrame(
            [("G01", 0, ""), ("G02", 1, "bad")],
            ["school_id_govt", "dq_has_critical_error", "failure_reason"],
        )

        failed = dq_split_failed_rows(df, "geolocation")
        assert failed.count() == 1
        assert failed.collect()[0]["school_id_govt"] == "G02"


# ════════════════════════════════════════════════════════════════════════
#  10. Full geolocation DQ pipeline — row_level_checks end-to-end
# ════════════════════════════════════════════════════════════════════════


class TestRowLevelChecksGeolocation:
    """GIVEN a bronze DataFrame with realistic school data,
    WHEN the full geolocation DQ pipeline runs (row_level_checks),
    THEN precision, duplicates, range, and completeness are all validated."""

    @pytest.mark.asyncio
    async def test_precision_and_range_checks_run_e2e(self, spark_session):
        """
        GIVEN:
          - School A: good precision (5 decimal places), valid range
          - School B: bad precision (2 decimal places), valid range
          - School C: good precision, invalid latitude (999)
        WHEN: row_level_checks runs
        THEN:
          - A passes precision and range
          - B fails precision
          - C fails range → critical error
        """
        bronze_schema = StructType(
            [
                StructField("school_id_govt", StringType()),
                StructField("school_id_giga", StringType()),
                StructField("school_name", StringType()),
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType()),
                StructField("education_level", StringType()),
                StructField("signature", StringType()),
            ]
        )
        bronze_data = [
            ("G01", "GIGA01", "School A", 10.12345, 20.12345, "Primary", "sig1"),
            ("G02", "GIGA02", "School B", 11.12, 21.12, "Secondary", "sig2"),
            ("G03", "GIGA03", "School C", 999.0, 22.12345, "Primary", "sig3"),
        ]
        bronze_df = spark_session.createDataFrame(bronze_data, bronze_schema)

        silver = spark_session.createDataFrame(
            [], StructType([StructField("school_id_govt", StringType())])
        )

        with (
            patch("src.data_quality_checks.geography.settings.DEPLOY_ENV", "local"),
        ):
            result = row_level_checks(
                df=bronze_df,
                dataset_type="geolocation",
                _country_code_iso3="BRA",
                silver=silver,
                mode="Create",
            )

        # The geolocation pipeline produces individual dq_* columns and
        # a final dq_has_critical_error column
        rows = {r["school_id_govt"]: r for r in result.collect()}

        # School A: good precision (5 decimals)
        assert int(rows["G01"]["dq_precision-latitude"]) == 0
        assert int(rows["G01"]["dq_precision-longitude"]) == 0

        # School B: bad precision (2 decimals < 5 required)
        assert int(rows["G02"]["dq_precision-latitude"]) == 1
        assert int(rows["G02"]["dq_precision-longitude"]) == 1

        # School C: latitude=999 is out-of-range → critical error
        assert rows["G03"]["dq_has_critical_error"] == 1
