from pathlib import Path

from pyspark import sql

from dagster import OpExecutionContext
from src.spark.config_expectations import Config
from src.utils.adls import ADLSFileClient
from src.utils.op_config import FileConfig


def convert_dq_checks_to_human_readeable_descriptions_and_upload(
    dq_results: sql.DataFrame,
    bronze: sql.DataFrame,
    config: FileConfig,
    context: OpExecutionContext,
) -> sql.DataFrame:
    config_expectations_instance = Config()
    adls_client = ADLSFileClient()

    columns_to_rename = {
        "dq_is_not_within_country": "Are the coordinates not within the country",
        "dq_duplicate_similar_name_same_level_within_110m_radius": "Are there duplicates across educational_level, lat_110, long_110 that has similar names as well",
        "dq_is_school_density_greater_than_5": "Is the the school density within the area is greater than 5.",
    }

    duplicate_columns = ["school_id_govt", "school_id_giga"]
    duplicate_desc = {
        f"dq_duplicate-{column}": f"Does column {column} have a duplicate"
        for column in duplicate_columns
    }

    ALL_COLUMNS = [
        *config_expectations_instance.NONEMPTY_COLUMNS_ALL,
        *config_expectations_instance.COLUMNS_EXCEPT_SCHOOL_ID_MASTER,
        *config_expectations_instance.COLUMNS_EXCEPT_SCHOOL_ID_GEOLOCATION,
        "signature",
    ]

    completeness_optional_desc = {
        f"dq_is_null_optional-{column}": f"Is nullable column {column} null"
        for column in ALL_COLUMNS
    }
    completeness_mandatory_desc = {
        f"dq_is_null_mandatory-{column}": f"Is non-nullable column {column} null"
        for column in ALL_COLUMNS
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
        for column, values in config_expectations_instance.DATA_TYPES
    }

    format_validation_numeric_desc = {
        f"dq_is_not_numeric-{column}": f"Is column {column} numeric"
        for column, values in config_expectations_instance.DATA_TYPES
    }

    format_validation_hash_desc = {
        f"dq_is_not_36_character_hash-{column}": f"Is column {column} a 36 character hash"
        for column, values in config_expectations_instance.DATA_TYPES
    }

    string_length_desc = {
        f"dq_is_string_more_than_255_characters-{column}": f"Is {column} more than 255 characters long"
        for column in bronze.columns
    }

    duplicate_all_except_checks_desc = {
        "dq_duplicate_all_except_school_code": "Are there duplicates across all columns except for School ID",
    }

    precision_check_desc = {
        f"dq_precision-{column}": f"Does column {column} have at least {value['min']} precision decimal places"
        for column, value in config_expectations_instance.PRECISION.items()
    }

    duplicate_set_checks_desc = {
        f"dq_duplicate_set-{'_'.join(column)}": f"Are there duplicates across these columns [{column}]"
        for column in config_expectations_instance.UNIQUE_SET_COLUMNS
    }

    duplicate_name_level_110_check_desc = {
        "dq_duplicate_name_level_within_110m_radius": "Are there duplicates across name, level, lat_110, and long_110",
    }

    column_relation_checks_desc = {
        "dq_column_relation_checks-cellular_coverage_availability_cellular_coverage_type": "Are the column relationship between the following columns [cellular_coverage_availability, cellular_coverage_type] as expected",
        "dq_column_relation_checks-connectivity_connectivity_RT_connectivity_govt_download_speed_contracted": "Are the column relationship between the following columns [connectivity, connectivity_RT, connectivity_govt, download_speed_contracted] as expected.",
        "dq_column_relation_checks-connectivity_govt_connectivity_govt_ingestion_timestamp": "Are the column relationship between the following columns [connectivity_govt, connectivity_govt_ingestion_timestamp] as expected",
        "dq_column_relation_checks-connectivity_govt_download_speed_contracted": "Are the column relationship between the following columns [connectivity_govt, download_speed_contracted] as excpected",
        "dq_column_relation_checks-connectivity_RT_connectivity_RT_datasource_connectivity_RT_ingestion_timestamp": "Are the column relationship between the following columns [connectivity_RT, connectivity_RT_datasource, connectivity_RT_ingestion_timestamp] as expected",
        "dq_column_relation_checks-electricity_availability_electricity_type": "Are the column relationship between the following columns [connectivity_govt, download_speed_contracted] as excpected",
        "dq_column_relation_checks-nearest_NR_id_nearest_NR_distance": "Are the column relationship between the following columns [nearest_NR_id, nearest_NR_distance] as expected",
        "dq_column_relation_checks-nearest_LTE_id_nearest_LTE_distance": "Are the column relationship between the following columns [nearest_LTE_id, nearest_LTE_distance] as expected",
        "dq_column_relation_checks-nearest_UMTS_id_nearest_UMTS_distance": "Are the column relationship between the following columns [nearest_UMTS_id, nearest_UMTS_distance] as expected",
        "dq_column_relation_checks-nearest_GSM_id_nearest_GSM_distance": "Are the column relationship between the following columns [nearest_GSM_id, nearest_GSM_distance] as expected",
    }

    critical_error_checks_desc = {
        "dq_is_null_mandatory-school_id_govt": "Is the Non-nullable column school_id_govt null",
        "dq_duplicate-school_id_govt": "Does column school_id_govt have a duplicate",
        "dq_duplicate-school_id_giga": "Does column school_id_giga have a duplicate",
        "dq_is_invalid_range-latitude": "Are columns latitude not between -90 and 90",
        "dq_is_invalid_range-longitude": "ARe columns longitude not between -180 and 180",
        "dq_is_not_within_country": "Are coordinates not within the country",
        "dq_is_not_create": "Did the user try to update a record even when selecting the create function",
        "dq_is_not_update": "Did the user try to create a record even when selecting the update function",
    }

    fb_percent_sum_to_100_checks_desc = {
        "dq_is_sum_of_percent_not_equal_100": "Is the sum of percent_2G, percent_3G, and percent_4G not equal to 100"
    }

    # combine {dq_codes : description} mapping
    columns_to_rename.update(duplicate_desc)
    columns_to_rename.update(completeness_optional_desc)
    columns_to_rename.update(completeness_mandatory_desc)
    columns_to_rename.update(domain_desc)
    columns_to_rename.update(range_desc)
    columns_to_rename.update(format_validation_alphanumeric_desc)
    columns_to_rename.update(format_validation_numeric_desc)
    columns_to_rename.update(format_validation_hash_desc)
    columns_to_rename.update(string_length_desc)
    columns_to_rename.update(duplicate_all_except_checks_desc)
    columns_to_rename.update(precision_check_desc)
    columns_to_rename.update(duplicate_set_checks_desc)
    columns_to_rename.update(duplicate_name_level_110_check_desc)
    columns_to_rename.update(column_relation_checks_desc)
    columns_to_rename.update(critical_error_checks_desc)
    columns_to_rename.update(fb_percent_sum_to_100_checks_desc)

    # create new dataframe with renamed columns
    for existing_col, new_col in columns_to_rename.items():
        if existing_col in dq_results.columns:
            dq_results = dq_results.withColumnRenamed(existing_col, new_col)
    dq_with_renamed_headers = dq_results
    dq_with_renamed_headers_pandas = dq_with_renamed_headers.toPandas()

    ## upload to new path
    upload_path = Path(config.destination_filepath)
    dataset = upload_path.parts[1]
    country_code = upload_path.parts[3]
    file_name = upload_path.name

    temp_filepath = f"data-quality-results/{dataset}/dq-human-readeable-descriptions/{country_code}/{file_name}"
    adls_client.upload_pandas_dataframe_as_file_from_asset(
        context=context, data=dq_with_renamed_headers_pandas, filepath=temp_filepath
    )
