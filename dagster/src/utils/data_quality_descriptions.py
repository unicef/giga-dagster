from pathlib import Path

from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.spark.config_expectations import Config
from src.utils.adls import ADLSFileClient
from src.utils.op_config import FileConfig


def human_readable_standard_checks(columns: list[str]) -> dict[str, str]:
    config_expectations_instance = Config()

    duplicate_desc = {
        f"dq_duplicate-{column}": f"Does the column {column} contain unique values"
        for column in config_expectations_instance.UNIQUE_COLUMNS_MASTER
    }

    completeness_optional_desc = {
        f"dq_is_null_optional-{column}": f"Is nullable column {column} null"
        for column in config_expectations_instance.NONEMPTY_COLUMNS_ALL
    }
    completeness_mandatory_desc = {
        f"dq_is_null_mandatory-{column}": f"Is non-nullable column {column} null"
        for column in config_expectations_instance.NONEMPTY_COLUMNS_ALL
    }

    domain_desc = {
        f"dq_is_invalid_domain-{column}": f"Does column {column} have values within [{','.join(values)}]"
        for column, values in config_expectations_instance.VALUES_DOMAIN_ALL.items()
    }

    range_desc = {
        f"dq_is_invalid_range-{column}": f"Does column {column} have values between {values['min']} and {values['max']}"
        for column, values in config_expectations_instance.VALUES_RANGE_ALL.items()
    }

    format_validation_alphanumeric_desc = {
        f"dq_is_not_alphanumeric-{column}": f"Is column {column} alphanumeric"
        for column, _ in config_expectations_instance.DATA_TYPES
    }

    format_validation_numeric_desc = {
        f"dq_is_not_numeric-{column}": f"Is column {column} numeric"
        for column, _ in config_expectations_instance.DATA_TYPES
    }

    format_validation_hash_desc = {
        f"dq_is_not_36_character_hash-{column}": f"Is column {column} a 36 character hash"
        for column, _ in config_expectations_instance.DATA_TYPES
    }

    string_length_desc = {
        f"dq_is_string_more_than_255_characters-{column}": f"Is {column} less than 255 characters long"
        for column in columns
    }

    standard_checks = {
        **duplicate_desc,
        **completeness_optional_desc,
        **completeness_mandatory_desc,
        **domain_desc,
        **range_desc,
        **format_validation_alphanumeric_desc,
        **format_validation_numeric_desc,
        **format_validation_hash_desc,
        **string_length_desc,
    }

    return standard_checks


def human_readable_geolocation_checks() -> dict[str, str]:
    config_expectations_instance = Config()

    create_update_desc = {
        "dq_is_not_create": "Did the user correctly use CREATE to add a non existing record",
        "dq_is_not_update": "Did the user correctly use UPDATE to update an existing record",
    }

    is_not_within_country_desc = {
        "dq_is_not_within_country": "Are the coordinates within the country",
    }

    similar_name_level_within_110_desc = {
        "dq_duplicate_similar_name_same_level_within_110m_radius": "Does the school have no similar name, education_level, lat, and long within a 110m radius",
    }

    school_density_desc = {
        "dq_is_school_density_greater_than_5": "Is the school density within the area less than 5.",
    }

    duplicate_all_except_checks_desc = {
        "dq_duplicate_all_except_school_code": "Does the check pass with no duplicates across all columns except for School ID",
    }

    precision_check_desc = {
        f"dq_precision-{column}": f"Does column {column} have at least {value['min']} precision decimal places"
        for column, value in config_expectations_instance.PRECISION.items()
    }

    duplicate_set_checks_desc = {
        f"dq_duplicate_set-{'_'.join(column)}": f"Does the check pass with no duplicates across these columns [{column}]"
        for column in config_expectations_instance.UNIQUE_SET_COLUMNS
    }

    duplicate_name_level_110_check_desc = {
        "dq_duplicate_name_level_within_110m_radius": "Does the school have no duplicate name, level, lat, and long within a 110m radius",
    }

    column_relation_checks_desc = {
        "dq_column_relation_checks-electricity_availability_electricity_type": "Are the column relationship between the following columns [connectivity_govt, download_speed_contracted] as excpected",
        "dq_column_relation_checks-connectivity_govt_download_speed_contracted": "Are the column relationship between the following columns [connectivity_govt, download_speed_contracted] as excpected",
    }

    critical_error_checks_desc = {
        "dq_is_null_mandatory-school_id_govt": "Is the Non-nullable column school_id_govt null",
        "dq_duplicate-school_id_govt": "Does the check pass with no duplicates in the school_id_govt column",
        "dq_duplicate-school_id_giga": "Does the check pass with no duplicates in the column school_id_giga column",
        "dq_is_invalid_range-latitude": "Is the school's latitude between -90 and 90",
        "dq_is_invalid_range-longitude": "Is the school's longitude between -180 and 180",
        "dq_is_not_within_country": "Are the coordinates within the country",
    }

    geolocation_checks = {
        **create_update_desc,
        **is_not_within_country_desc,
        **similar_name_level_within_110_desc,
        **school_density_desc,
        **duplicate_all_except_checks_desc,
        **precision_check_desc,
        **duplicate_set_checks_desc,
        **duplicate_name_level_110_check_desc,
        **column_relation_checks_desc,
        **critical_error_checks_desc,
    }

    return geolocation_checks


def human_readable_coverage_coverage_itu_checks() -> dict[str, str]:
    config_expectations_instance = Config()

    column_relation_checks_desc = {
        "dq_column_relation_checks-nearest_NR_id_nearest_NR_distance": "Are the column relationship between the following columns [nearest_NR_id, nearest_NR_distance] as expected",
        "dq_column_relation_checks-nearest_LTE_id_nearest_LTE_distance": "Are the column relationship between the following columns [nearest_LTE_id, nearest_LTE_distance] as expected",
        "dq_column_relation_checks-nearest_UMTS_id_nearest_UMTS_distance": "Are the column relationship between the following columns [nearest_UMTS_id, nearest_UMTS_distance] as expected",
        "dq_column_relation_checks-nearest_GSM_id_nearest_GSM_distance": "Are the column relationship between the following columns [nearest_GSM_id, nearest_GSM_distance] as expected",
    }

    CONFIG_NONEMPTY_COLUMNS_COVERAGE_AND_COVERAGE_ITU = list(
        {
            *config_expectations_instance.NONEMPTY_COLUMNS_COVERAGE,
            *config_expectations_instance.NONEMPTY_COLUMNS_COVERAGE_ITU,
        }
    )

    critical_error_checks_desc = {
        "dq_duplicate-school_id_giga": "Does the check pass with no duplicates in the column school_id_giga column",
    }

    for column in CONFIG_NONEMPTY_COLUMNS_COVERAGE_AND_COVERAGE_ITU:
        critical_error_checks_desc[f"dq_is_nul_mandatory-{column}"] = (
            f"Does the check pass with no duplicates in the {column} column"
        )

    coverage_and_coverage_itu_checks = {
        **column_relation_checks_desc,
        **critical_error_checks_desc,
    }

    return coverage_and_coverage_itu_checks


def human_readable_coverage_fb_checks() -> dict[str, str]:
    config_expectations_instance = Config()

    fb_percent_sum_to_100_checks_desc = {
        "dq_is_sum_of_percent_not_equal_100": "Is the sum of percent_2G, percent_3G, and percent_4G columns equal to 100"
    }

    critical_error_checks_desc = {
        "dq_duplicate-school_id_giga": "Does the check pass with no duplicates in the column school_id_giga column",
    }

    for column in config_expectations_instance.NONEMPTY_COLUMNS_COVERAGE_FB:
        critical_error_checks_desc[f"dq_is_nul_mandatory-{column}"] = (
            f"Does the check pass with no duplicates in the {column} column"
        )

    coverage_fb_checks = {
        **fb_percent_sum_to_100_checks_desc,
        **critical_error_checks_desc,
    }

    return coverage_fb_checks


def convert_dq_checks_to_human_readeable_descriptions_and_upload(
    dq_results: sql.DataFrame,
    dataset_type: str,
    bronze: sql.DataFrame,
    config: FileConfig,
    context: OpExecutionContext,
):
    adls_client = ADLSFileClient()
    columns = bronze.columns
    columns_to_rename = {}

    if dataset_type == "geolocation":
        columns_to_rename = {
            **human_readable_geolocation_checks(),
            **human_readable_standard_checks(columns=columns),
        }

    elif dataset_type in ["coverage", "coverage_itu"]:
        columns_to_rename = {
            **human_readable_coverage_coverage_itu_checks(),
            **human_readable_standard_checks(columns=columns),
        }

    elif dataset_type == "coverage_fb":
        columns_to_rename = {
            **human_readable_standard_checks(columns=columns),
            **human_readable_coverage_fb_checks(),
        }

    # combine {dq_codes : description} mapping

    # create new dataframe with renamed columns
    for existing_col, new_col in columns_to_rename.items():
        if existing_col in dq_results.columns:
            dq_results = dq_results.withColumn(
                existing_col,
                f.when(f.col(existing_col) == 1, "No").otherwise(
                    f.when(f.col(existing_col) == 0, "Yes")
                ),
            )

            dq_results = dq_results.withColumnRenamed(existing_col, new_col)
    dq_with_renamed_headers = dq_results
    dq_with_renamed_headers_pandas = dq_with_renamed_headers.toPandas()

    ## upload to new path
    upload_path = Path(config.destination_filepath)
    dataset = upload_path.parts[1]
    country_code = upload_path.parts[3]
    file_name = upload_path.name

    temp_filepath = f"data-quality-results/{dataset}/dq-human-readable-descriptions/{country_code}/{file_name}"
    adls_client.upload_pandas_dataframe_as_file(
        context=context, data=dq_with_renamed_headers_pandas, filepath=temp_filepath
    )


def handle_rename_dq_has_critical_error_column(
    null_mandatory_columns: list[str],
) -> dict[str, str]:
    human_readeable_non_empty_columns = {}

    for column in null_mandatory_columns:
        human_readeable_non_empty_columns[f"dq_is_null_mandatory-{column}"] = (
            f"Non-nullable column {column} is null"
        )

    human_readeable_extended_master_geolocation_columns_mapping = {
        "dq_duplicate-school_id_govt": "Column school_id_govt has a duplicate",
        "dq_duplicate-school_id_giga": "Column school_id_giga has a duplicate",
        "dq_is_invalid_range-latitude": "Column latitude is not between -90 and 90",
        "dq_is_invalid_range-longitude": "Column longitude is not between -180 and 180",
        "dq_is_not_within_country": "Coordinates is not within the country",
    }
    human_readeable_create_update_checks_mapping = {
        "dq_is_not_create": "Tried creating a new school_id_giga that already exists - must use UPDATE instead",
        "dq_is_not_update": "Tried updating a school_id_giga that does not exist - must use CREATE instead",
    }

    full_human_readable_mapping = {
        **human_readeable_non_empty_columns,
        **human_readeable_extended_master_geolocation_columns_mapping,
        **human_readeable_create_update_checks_mapping,
    }

    return full_human_readable_mapping
