"""Derivation of aggregate columns from their components.

Bronze carries every uploaded value as a string, so the dataframes here are
built as strings too, which is also what keeps the type-preservation cases
honest.
"""

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.spark.aggregate_derivations import (
    aggregate_relation_check_name,
    aggregate_rules,
    availability_from_count,
    derive_aggregate_columns,
    present_parts,
    sum_of_parts,
)

STRING = StringType()


def _df(spark, columns: list[str], rows: list[tuple], data_type=STRING):
    schema = StructType([StructField(name, data_type, True) for name in columns])
    return spark.createDataFrame(rows, schema=schema)


def _single(df, column):
    return df.select(column).collect()[0][0]


class TestSumDerivation:
    def test_fills_target_when_null(self, spark):
        df = _df(
            spark,
            ["num_students", "num_students_girls", "num_students_boys"],
            [(None, "40", "50")],
        )
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "num_students") == "90"

    def test_keeps_supplied_value_even_when_it_contradicts(self, spark):
        df = _df(
            spark,
            ["num_students", "num_students_girls", "num_students_boys"],
            [("100", "40", "50")],
        )
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "num_students") == "100"

    def test_sums_when_one_component_is_null(self, spark):
        df = _df(
            spark,
            ["num_students", "num_students_girls", "num_students_boys"],
            [(None, "40", None)],
        )
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "num_students") == "40"

    def test_stays_null_when_every_component_is_null(self, spark):
        df = _df(
            spark,
            ["num_students", "num_students_girls", "num_students_boys"],
            [(None, None, None)],
        )
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "num_students") is None

    def test_zero_components_derive_zero_not_null(self, spark):
        df = _df(
            spark,
            ["num_students", "num_students_girls", "num_students_boys"],
            [(None, "0", "0")],
        )
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "num_students") == "0"


class TestAvailabilityDerivation:
    def test_yes_when_counter_is_positive(self, spark):
        df = _df(spark, ["computer_availability", "num_computers"], [(None, "3")])
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "computer_availability") == "Yes"

    def test_no_when_counter_is_zero(self, spark):
        df = _df(spark, ["computer_availability", "num_computers"], [(None, "0")])
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "computer_availability") == "No"

    def test_null_when_counter_is_null(self, spark):
        df = _df(spark, ["computer_availability", "num_computers"], [(None, None)])
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "computer_availability") is None

    def test_device_availability_sums_both_counters(self, spark):
        df = _df(
            spark,
            ["device_availability", "num_computers", "num_tablets"],
            [(None, "0", "5")],
        )
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "device_availability") == "Yes"

    def test_device_availability_no_when_both_counters_are_zero(self, spark):
        df = _df(
            spark,
            ["device_availability", "num_computers", "num_tablets"],
            [(None, "0", "0")],
        )
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "device_availability") == "No"

    def test_keeps_supplied_value(self, spark):
        df = _df(spark, ["computer_availability", "num_computers"], [("No", "3")])
        result = derive_aggregate_columns(df, "geolocation")
        assert _single(result, "computer_availability") == "No"


class TestGuards:
    def test_no_op_when_no_component_column_exists(self, spark):
        df = _df(spark, ["num_students", "school_id_govt"], [(None, "abc")])
        result = derive_aggregate_columns(df, "geolocation")
        assert result.columns == df.columns
        assert _single(result, "num_students") is None

    def test_no_op_when_target_column_is_absent(self, spark):
        # Deriving a column outside the schema would be dropped by staging.
        df = _df(spark, ["num_students_girls", "num_students_boys"], [("40", "50")])
        result = derive_aggregate_columns(df, "geolocation")
        assert "num_students" not in result.columns

    def test_no_op_for_unconfigured_dataset_type(self, spark):
        df = _df(
            spark,
            ["num_students", "num_students_girls", "num_students_boys"],
            [(None, "40", "50")],
        )
        result = derive_aggregate_columns(df, "qos")
        assert _single(result, "num_students") is None

    def test_preserves_target_column_type(self, spark):
        df = _df(
            spark,
            ["num_students", "num_students_girls", "num_students_boys"],
            [(None, 40, 50)],
            data_type=IntegerType(),
        )
        result = derive_aggregate_columns(df, "geolocation")
        assert isinstance(result.schema["num_students"].dataType, IntegerType)
        assert _single(result, "num_students") == 90


class TestExpressionHelpers:
    def test_present_parts_filters_absent_columns(self, spark):
        df = _df(spark, ["num_computers"], [("1",)])
        assert present_parts(df, ["num_computers", "num_tablets"]) == ["num_computers"]

    def test_sum_of_parts_is_null_without_any_component(self, spark):
        df = _df(spark, ["school_id_govt"], [("abc",)])
        result = df.select(sum_of_parts(df, ["num_computers"]).alias("total"))
        assert _single(result, "total") is None

    def test_availability_from_count_preserves_null(self, spark):
        df = _df(spark, ["num_computers"], [(None,)])
        result = df.select(
            availability_from_count(sum_of_parts(df, ["num_computers"])).alias("a")
        )
        assert _single(result, "a") is None


class TestRuleConfiguration:
    def test_ticket_rules_are_configured(self):
        rules = {target: parts for target, parts, _ in aggregate_rules("geolocation")}
        assert rules["num_students"] == ["num_students_girls", "num_students_boys"]
        assert rules["num_teachers"] == ["num_teachers_female", "num_teachers_male"]
        assert "num_computers" not in rules  # counters are components, not targets
        assert rules["computer_availability"] == ["num_computers"]
        assert rules["device_availability"] == ["num_computers", "num_tablets"]

    def test_master_and_geolocation_share_the_same_rules(self):
        assert aggregate_rules("master") == aggregate_rules("geolocation")

    @pytest.mark.parametrize("dataset_type", ["coverage", "qos", "reference"])
    def test_other_dataset_types_have_no_rules(self, dataset_type):
        assert aggregate_rules(dataset_type) == []

    def test_check_name_matches_repository_convention(self):
        assert (
            aggregate_relation_check_name("num_students", ["a", "b"])
            == "dq_column_relation_checks-num_students_a_b"
        )
