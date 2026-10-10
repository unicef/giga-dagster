"""Column relation checks generated from the aggregate rules.

Parametrised over the configuration so a rule added later is covered without
touching this file.
"""

import pytest
from pyspark.sql.types import StringType, StructField, StructType
from src.data_quality_checks.column_relation import column_relation_checks
from src.spark.aggregate_derivations import (
    aggregate_relation_check_name,
    aggregate_rules,
)

GEOLOCATION_RULES = aggregate_rules("geolocation")
RULE_IDS = [target for target, _, _ in GEOLOCATION_RULES]


def _df(spark, values: dict[str, str]):
    schema = StructType([StructField(name, StringType(), True) for name in values])
    return spark.createDataFrame([tuple(values.values())], schema=schema)


def _flag(spark, values: dict[str, str], check_name: str):
    result = column_relation_checks(_df(spark, values), "geolocation")
    return result.select(check_name).collect()[0][0]


def _consistent_and_inconsistent(target, parts, is_availability):
    """Return (consistent, inconsistent) column values for a rule."""
    components = {part: "1" for part in parts}
    if is_availability:
        return (
            {target: "Yes", **components},
            {target: "No", **components},
        )
    return (
        {target: str(len(parts)), **components},
        {target: "999", **components},
    )


@pytest.mark.parametrize(
    ("target", "parts", "is_availability"), GEOLOCATION_RULES, ids=RULE_IDS
)
class TestAggregateRelationChecks:
    def test_consistent_row_passes(self, spark, target, parts, is_availability):
        consistent, _ = _consistent_and_inconsistent(target, parts, is_availability)
        check = aggregate_relation_check_name(target, parts)
        assert _flag(spark, consistent, check) == 0

    def test_inconsistent_row_is_flagged(self, spark, target, parts, is_availability):
        _, inconsistent = _consistent_and_inconsistent(target, parts, is_availability)
        check = aggregate_relation_check_name(target, parts)
        assert _flag(spark, inconsistent, check) == 1

    def test_null_target_is_not_flagged(self, spark, target, parts, is_availability):
        # A missing aggregate is derived upstream, not reported here.
        values = {target: None, **{part: "1" for part in parts}}
        check = aggregate_relation_check_name(target, parts)
        assert _flag(spark, values, check) == 0

    def test_null_components_are_not_flagged(
        self, spark, target, parts, is_availability
    ):
        consistent, inconsistent = _consistent_and_inconsistent(
            target, parts, is_availability
        )
        values = {**inconsistent, **{part: None for part in parts}}
        check = aggregate_relation_check_name(target, parts)
        assert _flag(spark, values, check) == 0

    def test_absent_columns_are_not_flagged(
        self, spark, target, parts, is_availability
    ):
        check = aggregate_relation_check_name(target, parts)
        assert _flag(spark, {"school_id_govt": "abc"}, check) == 0

    def test_check_is_always_emitted(self, spark, target, parts, is_availability):
        # The column must exist even when it cannot be evaluated, so the
        # dq_results map has a stable set of keys across uploads.
        result = column_relation_checks(
            _df(spark, {"school_id_govt": "abc"}), "geolocation"
        )
        assert aggregate_relation_check_name(target, parts) in result.columns


class TestAvailabilitySpellings:
    """Countries spell yes/no in several ways; agreement must not depend on it."""

    CHECK = aggregate_relation_check_name("computer_availability", ["num_computers"])

    @pytest.mark.parametrize(
        "supplied", ["yes", "YES", "Yes", " Yes ", "true", "TRUE", "Y", "1"]
    )
    def test_truthy_spellings_agree_with_a_positive_count(self, spark, supplied):
        values = {"computer_availability": supplied, "num_computers": "3"}
        assert _flag(spark, values, self.CHECK) == 0

    @pytest.mark.parametrize(
        "supplied", ["no", "NO", "No", "false", "FALSE", "False", "N", "0"]
    )
    def test_falsy_spellings_agree_with_a_zero_count(self, spark, supplied):
        values = {"computer_availability": supplied, "num_computers": "0"}
        assert _flag(spark, values, self.CHECK) == 0

    @pytest.mark.parametrize("supplied", ["false", "FALSE", "N", "0"])
    def test_falsy_spellings_still_contradict_a_positive_count(self, spark, supplied):
        values = {"computer_availability": supplied, "num_computers": "5"}
        assert _flag(spark, values, self.CHECK) == 1

    @pytest.mark.parametrize("supplied", ["Unknown", "n/a", "maybe"])
    def test_unrecognised_values_are_left_to_the_domain_checks(self, spark, supplied):
        values = {"computer_availability": supplied, "num_computers": "5"}
        assert _flag(spark, values, self.CHECK) == 0

    def test_decimal_counts_from_pandas_are_understood(self, spark):
        # pandas turns an int column with blanks into float64, so bronze carries
        # "5.0" rather than "5".
        values = {"computer_availability": "No", "num_computers": "5.0"}
        assert _flag(spark, values, self.CHECK) == 1


class TestExistingChecksAreUntouched:
    def test_electricity_relation_still_present(self, spark):
        values = {"electricity_availability": "Yes", "electricity_type": None}
        result = column_relation_checks(_df(spark, values), "geolocation")
        flag = result.select(
            "dq_column_relation_checks-electricity_availability_electricity_type"
        ).collect()[0][0]
        assert flag == 1
