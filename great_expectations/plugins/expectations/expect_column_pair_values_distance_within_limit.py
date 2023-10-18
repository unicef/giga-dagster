from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration

# from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (  # SparkDFExecutionEngine,; SqlAlchemyExecutionEngine,
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most ColumnPairMapExpectations, the main business logic for calculation will live in this class.
class ColumnPairValuesToBeFarApart(ColumnPairMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_pair_values.far_apart"
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ()

    # This method implements the core logic for the PandasExecutionEngine
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        print(column_A)
        return abs(column_A - column_B) == 3

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_A, column_B, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnPairValuesToBeFarApart(ColumnPairMapExpectation):
    """Expect the pair of points to not be within a certain distance."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "col_a": [3, 0, 1, 2, 3, 2],
                "col_b": [0, -3, 4, -1, 0, 1],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "col_a", "column_B": "col_b", "mostly": 0.8},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "col_a", "column_B": "col_b", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_pair_values.far_apart"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_A",
        "column_B",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnPairValuesToBeFarApart().print_diagnostic_checklist()
