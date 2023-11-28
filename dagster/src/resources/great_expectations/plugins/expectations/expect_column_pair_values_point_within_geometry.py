"""
This is a template for creating custom ColumnPairMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations
"""

from typing import Optional

import pandas as pd
from check_functions import (
    is_within_boundary_distance,
    is_within_country_gadm,
    is_within_country_geopy,
)
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
class ColumnPairValuesToBeWithinGeometry(ColumnPairMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_pair_values.within_geometry"
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ()

    # This method implements the core logic for the PandasExecutionEngine
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        results = []
        country_code_iso3 = kwargs.get("country_code", None)
        for latitude, longitude in zip(column_A, column_B):
            is_within = (
                is_within_country_gadm(latitude, longitude, country_code_iso3)
                or is_within_country_geopy(latitude, longitude, country_code_iso3)
                or is_within_boundary_distance(latitude, longitude, country_code_iso3)
            )
            results.append(is_within)
        return pd.Series(results)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_A, column_B, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnPairValuesToBeWithinGeometry(ColumnPairMapExpectation):
    """Expect the point to be within the geospatial shape."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                # latitude
                "col_a": [
                    -88.19756,
                    -88.19756,
                    -88.19756,
                    -88.19756,
                    -88.19756,
                ],
                # longitude
                "col_b": [
                    17.49952,
                    17.49952,
                    17.49952,
                    17.49952,
                    17.49952,
                ],
                "country": "BLZ",
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column_A": "col_a",
                        "column_B": "col_b",
                        "iso_country_code": "country",
                        "mostly": 0.8,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column_A": "col_a",
                        "column_B": "col_b",
                        "iso_country_code": "country",
                        "mostly": 1,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_pair_values.within_geometry"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_A",
        "column_B",
        "iso_country_code",
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
    ExpectColumnPairValuesToBeWithinGeometry().print_diagnostic_checklist()
