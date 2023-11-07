import tools_expectations as gx_tools
from config_expectations import (  # CONFIG_COLUMN_SUM,; CONFIG_FIVE_DECIMAL_PLACES,; CONFIG_NOT_SIMILAR_COLUMN,; CONFIG_PAIR_AVAILABILITY,; CONFIG_VALUES_RANGE_COVERAGE_ITU,; CONFIG_VALUES_TYPE,
    CONFIG_NONEMPTY_COLUMNS,
    CONFIG_UNIQUE_COLUMNS,
    CONFIG_UNIQUE_SET_COLUMNS,
    CONFIG_VALUES_OPTIONS,
    CONFIG_VALUES_RANGE,
)

import great_expectations as gx

# This file is for setting the expectations for school geolocation data
# Instead of directly editing the expectation file in great_expectations/expectatios,
# the expectations should ideally be set through the code to ensure that the expectations are in the proper format.
# - The expectation module (including the custom expectation) actually exist
# - The parameters set in the expectations are the expected data types

# To do:
# 1. Separate the config info into a settings json.
# 2. Turn this into a generic function that would be used for configuring / setting the expectations.

# Initial Expectation Creation
# context = gx.data_context.FileDataContext.create(context_root_dir)
# context = gx.get_context()
context_root_dir = "./great_expectations/"
context = gx.get_context(context_root_dir=context_root_dir)
suite = context.add_or_update_expectation_suite(
    expectation_suite_name="expectation_school_geolocation"
)

# Notes: These value options may eventually be variables on the UI that the user can set.

# Setting expectations
# Single Column Expectations
for column in CONFIG_UNIQUE_COLUMNS:
    gx_config = gx_tools.add_single_column_expectation(
        "expect_column_values_to_be_unique", column
    )
    suite.add_expectation(expectation_configuration=gx_config)

for column in CONFIG_NONEMPTY_COLUMNS:
    gx_config = gx_tools.add_single_column_expectation(
        "expect_column_values_to_not_be_null", column
    )
    suite.add_expectation(expectation_configuration=gx_config)

# Custom Single Column Expectations
# for column in CONFIG_NOT_SIMILAR_COLUMN:
#     gx_config = gx_tools.add_single_column_expectation(
#         "expect_column_values_to_be_not_similar", column
#     )
#     suite.add_expectation(expectation_configuration=gx_config)

# for column in CONFIG_FIVE_DECIMAL_PLACES:
#     gx_config = gx_tools.add_single_column_expectation(
#         "expect_column_values_to_have_min_decimal_digits", column
#     )
#     suite.add_expectation(expectation_configuration=gx_config)

# Single Column Expectations w/ parameters
for value in CONFIG_VALUES_RANGE:
    gx_config = gx_tools.add_expect_column_values_to_be_between(
        value, CONFIG_VALUES_RANGE[value]["min"], CONFIG_VALUES_RANGE[value]["max"]
    )
    suite.add_expectation(expectation_configuration=gx_config)

for value in CONFIG_VALUES_OPTIONS:
    gx_config = gx_tools.add_expect_column_values_to_be_in_set(
        value, CONFIG_VALUES_OPTIONS[value]
    )
    suite.add_expectation(expectation_configuration=gx_config)

# # To do: Find out the type names (e.g. int or INTEGER?)
# for column in CONFIG_VALUES_TYPE:
#     gx_config = gx_tools.add_expect_column_values_to_be_of_type(column['column'], column['type'])
#     suite.add_expectation(gx_config)

# for item in CONFIG_COLUMN_SUM:
#     gx_config = gx_tools.add_expect_multicolumn_sum_values_to_be_equal_to_single_column(column_list)
#     suite.add_expectation(gx_config)

# Custom Expectations
for column_list in CONFIG_UNIQUE_SET_COLUMNS:
    gx_config = gx_tools.add_unique_columns_expectation(column_list)
    suite.add_expectation(expectation_configuration=gx_config)

# for column in CONFIG_PAIR_AVAILABILITY:
#     gx_config = gx_tools.add_expect_pair_availability(
#         column["availability_column"], column["value_column"], 1.0
#     )
#     suite.add_expectation(gx_config)

print("CONTEXT")
print(context)
print("SUITE")
print(suite)
result = context.save_expectation_suite(expectation_suite=suite)
print("RESULT")
print(result)
