# from expect_column_pair_values_same_availability import ExpectColumnPairValuesSameAvailability
# from expect_column_values_to_be_not_similar import ExpectColumnValuesToBeNotSimilar
from great_expectations.core.expectation_configuration import ExpectationConfiguration


# To Do: Simplify functions into string mapping
def add_expectation(expectation_type, kwargs, content):
    gx_config = ExpectationConfiguration(
        expectation_type=expectation_type,
        kwargs=kwargs,
        meta={
            "notes": {
                "format": "markdown",
                "content": content,
            }
        },
    )
    return gx_config


def add_expect_table_columns_to_match_set(column_set, exact_match=False):
    gx_config = ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_set",
        kwargs={"column_set": column_set, "exact_match": exact_match},
        meta={
            "notes": {
                "format": "markdown",
                "content": "Checks if the mandatory fields ({}) are present".format(
                    column_set
                ),
            }
        },
    )
    return gx_config


def add_unique_column_expectation(column_name):
    gx_config = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": column_name},
        meta={
            "notes": {
                "format": "markdown",
                "content": "This indicates if the {} is unique".format(column_name),
            }
        },
    )
    return gx_config


def add_unique_columns_expectation(column_names):
    gx_config = ExpectationConfiguration(
        expectation_type="expect_compound_columns_to_be_unique",
        kwargs={"column_list": column_names},
        meta={
            "notes": {
                "format": "markdown",
                "content": "This indicates the uniqueness of compound columns {}".format(
                    column_names
                ),
            }
        },
    )
    return gx_config


def add_expect_column_values_to_not_be_null(column_name):
    gx_config = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": column_name},
        meta={
            "notes": {
                "format": "markdown",
                "content": "This expects the column {} to have a value.".format(
                    column_name
                ),
            }
        },
    )
    return gx_config


def add_expect_column_values_to_be_between(column_name, min_value, max_value):
    gx_config = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": column_name,
            "min_value": min_value,
            "max_value": max_value
            # column (str): The column name.
            # min_value (comparable type or None): The minimum value for a column entry.
            # max_value (comparable type or None): The maximum value for a column entry.
            # strict_min (boolean): If True, values must be strictly larger than min_value, default=False
            # strict_max (boolean): If True, values must be strictly smaller than max_value, default=False
        },
        meta={
            "notes": {
                "format": "markdown",
                "content": "This expects the column {} to have a value between {} and {}.".format(
                    column_name, min_value, max_value
                ),
            }
        },
    )
    return gx_config


def add_expect_pair_availability(availability_column, value_column, mostly):
    # ExpectColumnPairValuesSameAvailability
    gx_config = ExpectationConfiguration(
        expectation_type="expect_column_pair_values_same_availability",  # column_pair_values.same_availability
        kwargs={
            "column_A": availability_column,
            "column_B": value_column,
            "mostly": mostly,
        },
        meta={
            "notes": {
                "format": "markdown",
                "content": "Indicates if {} has value when {} is 'Yes'".format(
                    value_column, availability_column
                ),
            }
        },
    )
    return gx_config


# To Test
def add_expect_column_values_to_be_of_type(column, type):
    gx_config = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": column, "type": type},
        meta={
            "notes": {
                "format": "markdown",
                "content": "Indicates if {} has date type of {}".format(column, type),
            }
        },
    )
    return gx_config


def add_expect_multicolumn_sum_values_to_be_equal_to_single_column(column_list):
    gx_config = ExpectationConfiguration(
        # To verify if expectation is already released or still in beta or experimental
        expectation_type="expect_multicolumn_sum_values_to_be_equal_to_single_column",
        kwargs={"column_list": column_list},
        meta={
            "notes": {
                "format": "markdown",
                "content": "Indicates if columns {} has value when {} is 'Yes'".format(
                    column_list[:-1], column_list[-1]
                ),
            }
        },
    )
    return gx_config


def add_expect_column_values_to_be_not_similar(column):
    gx_config = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_not_similar",
        kwargs={"column": column},
        meta={
            "notes": {
                "format": "markdown",
                "content": "Indicates if {} is without any similar name.".format(
                    column
                ),
            }
        },
    )
    return gx_config


# def add_expect_column_values_number_of_decimal_places_to_equal()
