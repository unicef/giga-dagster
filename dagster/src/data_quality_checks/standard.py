from typing import Any

from pyspark import sql
from pyspark.sql import (
    Window,
    functions as f,
)

from dagster import OpExecutionContext
from src.data_quality_checks.config import (
    CONFIG_NONEMPTY_COLUMNS,
    CONFIG_UNIQUE_COLUMNS,
    CONFIG_VALUES_DOMAIN,
    CONFIG_VALUES_RANGE,
)
from src.spark.config_expectations import config
from src.utils.logger import get_context_with_fallback_logger


def duplicate_checks(
    df: sql.DataFrame, config_column_list: list[str], context: OpExecutionContext = None
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate checks...")

    column_actions = {}
    for column in config_column_list:
        column_actions[f"dq_duplicate-{column}"] = f.when(
            f.count(column).over(Window.partitionBy(f.col(column))) > 1,
            1,
        ).otherwise(0)

    return df.withColumns(column_actions)


def completeness_checks(
    df: sql.DataFrame, config_column_list: list[str], context: OpExecutionContext = None
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running completeness checks...")

    # list of dq columns for exclusion
    dq_columns = [col for col in df.columns if col.startswith("dq_")]

    column_actions = {}
    # optional columns
    for column in df.columns:
        if column not in config_column_list and column not in dq_columns:
            column_actions[f"dq_is_null_optional-{column}"] = f.when(
                f.isnull(f.col(column)),
                1,
            ).otherwise(0)

    # mandatory columns
    for column in config_column_list:
        check_name = f"dq_is_null_mandatory-{column}"
        if column in df.columns:
            column_actions[check_name] = f.when(
                f.col(column).isNull(),
                1,
            ).otherwise(0)
        else:
            column_actions[check_name] = f.lit(1)

    return df.withColumns(column_actions)


def range_checks(
    df: sql.DataFrame,
    config_column_list: dict[str, Any],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running range checks...")

    column_actions = {}
    for column in config_column_list:
        check_name = f"dq_is_invalid_range-{column}"
        if column in df.columns:
            column_actions[check_name] = f.when(
                f.col(f"{column}").between(
                    config_column_list[column]["min"],
                    config_column_list[column]["max"],
                ),
                0,
            ).otherwise(1)
        else:
            column_actions[check_name] = f.lit(1)

    return df.withColumns(column_actions)


def domain_checks(
    df: sql.DataFrame,
    config_column_list: dict[str, Any],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running domain checks...")

    column_actions = {}
    for column in config_column_list:
        check_name = f"dq_is_invalid_domain-{column}"
        if column in df.columns:
            column_actions[check_name] = f.when(
                f.lower(f.col(f"{column}")).isin(
                    [x.lower() for x in config_column_list[column]],
                ),
                0,
            ).otherwise(1)
        else:
            column_actions[check_name] = f.lit(1)
    return df.withColumns(column_actions)


def format_validation_checks(df, context: OpExecutionContext = None):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running format validation checks...")

    column_actions = {}
    for column, dtype in config.DATA_TYPES:
        if column in df.columns and dtype == "STRING":
            column_actions[f"dq_is_not_alphanumeric-{column}"] = f.when(
                f.regexp_extract(f.col(column), ".+", 0) != "",
                0,
            ).otherwise(1)
        if column in df.columns and dtype in [
            "INT",
            "DOUBLE",
            "LONG",
            "TIMESTAMP",
        ]:  # included timestamp based on luke's code
            column_actions[f"dq_is_not_numeric-{column}"] = f.when(
                f.regexp_extract(f.col(column), r"^-?\d+(\.\d+)?$", 0) != "",
                0,
            ).otherwise(1)   
        # special format validation for school_id_giga
        if column in df.columns and column == "school_id_giga":
            column_actions[f"dq_is_not_36_character_hash-{column}"] = f.when(
                f.regexp_extract(f.col(column), r"\b([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})\b", 0) != "",
                0,
            ).otherwise(1)  

    return df.withColumns(column_actions)


def is_string_less_than_255_characters_check(
    df: sql.DataFrame,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running character length checks...")

    column_actions = {}
    for column in df.columns:
        if column != "school_id_giga" and not column.startswith("dq"):
            column_actions[f"dq_is_string_less_than_255_characters-{column}"] = f.when(
                f.length(column) > 255,
                1,
            ).otherwise(0)

    return df.withColumns(column_actions)

        
def standard_checks(
    df: sql.DataFrame,
    dataset_type: str,
    context: OpExecutionContext = None,
    duplicate=True,
    completeness=True,
    domain=True,
    range_=True,
    format_=True,
    string_length=True,
):
    if duplicate:
        df = duplicate_checks(df, CONFIG_UNIQUE_COLUMNS[dataset_type], context)
    if completeness:
        df = completeness_checks(df, CONFIG_NONEMPTY_COLUMNS[dataset_type], context)
    if domain:
        df = domain_checks(df, CONFIG_VALUES_DOMAIN[dataset_type], context)
    if range_:
        df = range_checks(df, CONFIG_VALUES_RANGE[dataset_type], context)
    if format_:
        df = format_validation_checks(df, context)
    if string_length:
        df = is_string_less_than_255_characters_check(df, context)

    return df
