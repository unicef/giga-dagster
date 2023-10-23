import tools_expectations as gx_tools

import great_expectations as gx

# This file is for setting the expectations for school geolocation data
# Instead of directly editing the expectation file in great_expectations/expectatios,
# the expectations should ideally be set through the code to ensure that the expectations are in the proper format.
# - The expectation module (including the custom expectation) actually exist
# - The parameters set in the expectations are the expected data types

# To do
# 1. Separate the config info into a settings json.
# 2. Turn this into a generic function that would be used for configuring / setting the expectations.

# Initial Expectation Creation
# full_path_to_project_directory = "./project/"
# context_root_dir = "/home/test/giga-dagster/great_expectations/"
context_root_dir = "./great_expectations/"
# context = gx.data_context.FileDataContext.create(context_root_dir)
# context = gx.get_context()
# # C:/Users/AvelineGermar/Documents/client-proj/UNICEF/project
context = gx.get_context(context_root_dir=context_root_dir)
# # suite = context.add_expectation_suite(expectation_suite_name="expectation_school_geolocation")
suite = context.add_or_update_expectation_suite(
    expectation_suite_name="expectation_school_geolocation"
)

CONFIG_UNIQUE_COLUMNS = ["school_id"]
CONFIG_UNIQUE_SET_COLUMNS = [
    ["school_id", "school_name", "education_level", "latitude", "longitude"],
    ["school_name", "education_level", "latitude", "longitude"],
]
CONFIG_NONEMPTY_COLUMNS = [
    "school_name",
    "longitude",
    "latitude",
    "education_level",
    "mobile_internet_generation",
    "internet_availability",
    "internet_type",
    "internet_speed_mbps",
]
CONFIG_VALUES_RANGE = [
    {"column": "internet_speed_mbps", "min": 1, "max": 20},
    {"column": "student_count", "min": 20, "max": 2500},
]

CONFIG_VALUES_TYPE = [{"column": "school_id", "type": "int64"}]

CONFIG_PAIR_AVAILABILITY = [
    {"availability_column": "internet_availability", "value_column": "internet_type"},
    {
        "availability_column": "internet_availability",
        "value_column": "internet_speed_mbps",
    },
]

CONFIG_COLUMN_SUM = [
    ["student_count_girls", "student_count_boys", "student_count_others"]
]

CONFIG_NOT_SIMILAR_COLUMN = ["school_name"]

# Setting expectations
for column in CONFIG_UNIQUE_COLUMNS:
    gx_config = gx_tools.add_unique_column_expectation(column)
    suite.add_expectation(expectation_configuration=gx_config)
    # suite.add_expectation(gx_config)

for column_list in CONFIG_UNIQUE_SET_COLUMNS:
    gx_config = gx_tools.add_unique_columns_expectation(column_list)
    # suite.add_expectation(expectation_configuration=gx_config)
    suite.add_expectation(gx_config)

for column in CONFIG_NONEMPTY_COLUMNS:
    gx_config = gx_tools.add_expect_column_values_to_not_be_null(column)
    # suite.add_expectation(expectation_configuration=gx_config)
    suite.add_expectation(gx_config)

for column in CONFIG_VALUES_RANGE:
    gx_config = gx_tools.add_expect_column_values_to_be_between(
        column["column"], column["min"], column["max"]
    )
    # suite.add_expectation(expectation_configuration=gx_config)
    suite.add_expectation(gx_config)

# # To do: Find out the type names (e.g. int or INTEGER?)
# for column in CONFIG_VALUES_TYPE:
#     gx_config = gx_tools.add_expect_column_values_to_be_of_type(column['column'], column['type'])
#     suite.add_expectation(gx_config)

# for item in CONFIG_COLUMN_SUM:
#     gx_config = gx_tools.add_expect_multicolumn_sum_values_to_be_equal_to_single_column(column_list)
#     suite.add_expectation(gx_config)

for column in CONFIG_PAIR_AVAILABILITY:
    gx_config = gx_tools.add_expect_pair_availability(
        column["availability_column"], column["value_column"], 1.0
    )
    suite.add_expectation(gx_config)

for column in CONFIG_NOT_SIMILAR_COLUMN:
    gx_config = gx_tools.add_expect_column_values_to_be_not_similar(column)
    suite.add_expectation(gx_config)

print("CONTEXT")
print(context)
print("SUITE")
print(suite)
result = context.save_expectation_suite(expectation_suite=suite)
print("RESULT")
print(result)
