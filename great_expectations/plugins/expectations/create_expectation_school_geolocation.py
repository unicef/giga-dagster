import datetime

import tools_expectations as gx_tools

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

# Single column checks
CONFIG_UNIQUE_COLUMNS = ["school_id", "giga_id_school"]
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
CONFIG_NOT_SIMILAR_COLUMN = ["school_name"]
CONFIG_FIVE_DECIMAL_PLACES = ["latitude", "longitude"]

# Multi-column checks
CONFIG_UNIQUE_SET_COLUMNS = [
    ["school_id", "school_name", "education_level", "latitude", "longitude"],
    ["school_name", "education_level", "latitude", "longitude"],
]

CONFIG_COLUMN_SUM = [
    ["student_count_girls", "student_count_boys", "student_count_others"]
]

# Single Column w/ parameters
date_today = datetime.date.today()
current_year = date_today.year

# For GOVERNMENT SCHOOL COLUMNS
CONFIG_VALUES_RANGE = [
    # Mandatory
    {"column": "internet_speed_mbps", "min": 1, "max": 20},
    {"column": "student_count", "min": 20, "max": 2500},
    {"column": "latitude", "min": -90, "max": 90},
    {"column": "longitude", "min": -180, "max": 180},
    {"column": "school_establishment_year", "min": 1000, "max": current_year},
    {"column": "latitude", "min": -90, "max": 90},
    {"column": "longitude", "min": -180, "max": 180},
    # Calculated
    {"column": "school_density", "min": 0, "max": 5},
    # Optional
    # {"column": "connectivity_speed_static", "min": 0, "max": 200},
    # {"column": "connectivity_speed_contracted", "min": 0, "max": 500},
    # {"column": "connectivity_latency_static", "min": 0, "max": 200},
    # {"column": "num_computers", "min": 0, "max": 500},
    # {"column": "num_computers_desired", "min": 0, "max": 1000},
    # {"column": "num_teachers", "min": 0, "max": 200},
    # {"column": "num_adm_personnel", "min": 0, "max": 200},
    # {"column": "num_students", "min": 0, "max": 10000},
    # {"column": "num_classrooms", "min": 0, "max": 200},
    # {"column": "num_latrines", "min": 0, "max": 200},
    # {"column": "school_data_collection_year", "min": 1000, "max": current_year},
]

CONFIG_VALUES_OPTIONS = [
    {"column": "computer_lab", "set": ["yes", "no"]},
    {"column": "electricity_availability", "set": ["yes", "no"]},
    {"column": "water_availability", "set": ["yes", "no"]},
    {"column": "cellular_network_availability", "set": ["yes", "no"]},
    {"column": "connectivity_availability", "set": ["yes", "no"]},
    {"column": "school_is_open", "set": ["yes", "no"]},
    {"column": "school_daily_check_app", "set": ["yes", "no"]},
    {"column": "2G_coverage", "set": ["true", "false"]},
    {"column": "3G_coverage", "set": ["true", "false"]},
    {"column": "4G_coverage", "set": ["true", "false"]},
    {
        "column": "education_level_isced",
        "set": [
            "childhood education",
            "primary education",
            "secondary education",
            "post secondary education",
        ],
    },
    {
        "column": "connectivity_type",
        "set": [
            "fiber",
            "xdsl",
            "wired",
            "cellular",
            "p2mp wireless",
            "p2p wireless",
            "satellite",
            "other",
        ],
    },
    {"column": "school_area_type", "set": ["urban", "rural"]},
    {"column": "school_type_public", "set": ["public", "not public"]},
    {
        "column": "electricity_type",
        "set": ["electrical grid", "diesel generator", "solar power station", "other"],
    },
    {
        "column": "school_data_collection_modality",
        "set": ["online", "in-person", "phone", "other"],
    },
    {"column": "cellular_network_type", "set": ["2g", "3g", "4g", "5g"]},
]

# For COVERAGE ITU dataset. To separate COVERAGE ITU expectations into different file.
CONFIG_VALUES_RANGE_COVERAGE_ITU = [
    {"column": "fiber_node_distance", "min": 0, "max": None},
    {"column": "microwave_node_distance", "min": 0, "max": None},
    {"column": "nearest_school_distance", "min": 0, "max": None},
    {"column": "schools_within_1km", "min": 0, "max": 20},
    {"column": "schools_within_1km", "min": 0, "max": 40},
    {"column": "schools_within_1km", "min": 0, "max": 60},
    {"column": "schools_within_1km", "min": 0, "max": 100},
]

CONFIG_VALUES_TYPE = [{"column": "school_id", "type": "int64"}]

# Column Pairs
CONFIG_PAIR_AVAILABILITY = [
    {"availability_column": "internet_availability", "value_column": "internet_type"},
    {
        "availability_column": "internet_availability",
        "value_column": "internet_speed_mbps",
    },
]

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
for column in CONFIG_VALUES_RANGE:
    gx_config = gx_tools.add_expect_column_values_to_be_between(
        column["column"], column["min"], column["max"]
    )
    suite.add_expectation(expectation_configuration=gx_config)

for column in CONFIG_VALUES_OPTIONS:
    gx_config = gx_tools.add_expect_column_values_to_be_in_set(
        column["column"], column["set"]
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
