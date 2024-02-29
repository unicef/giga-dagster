import io
import json
from datetime import UTC, datetime
from difflib import SequenceMatcher
from functools import reduce
from operator import add
from typing import Any

# Geospatial
import country_converter as coco
import geopandas as gpd
from pyspark import sql

# Spark functions
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

import src.schemas

# Name Similarity
from azure.storage.blob import BlobServiceClient
from dagster import OpExecutionContext
from src.schemas import BaseSchema

# Auth
from src.settings import settings
from src.spark.config_expectations import (
    # Custom Check Configs
    CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_GEOLOCATION,
    CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_MASTER,
    # Data Quality Checks Descriptions
    CONFIG_DATA_QUALITY_CHECKS_DESCRIPTIONS,
    # Data Type Configs
    CONFIG_DATA_TYPES,
    CONFIG_NONEMPTY_COLUMNS_COVERAGE,
    CONFIG_NONEMPTY_COLUMNS_COVERAGE_FB,
    CONFIG_NONEMPTY_COLUMNS_COVERAGE_ITU,
    CONFIG_NONEMPTY_COLUMNS_GEOLOCATION,
    # Completeness Configs
    CONFIG_NONEMPTY_COLUMNS_MASTER,
    CONFIG_NONEMPTY_COLUMNS_REFERENCE,
    CONFIG_PRECISION,
    CONFIG_UNIQUE_COLUMNS_COVERAGE,
    CONFIG_UNIQUE_COLUMNS_COVERAGE_FB,
    CONFIG_UNIQUE_COLUMNS_COVERAGE_ITU,
    CONFIG_UNIQUE_COLUMNS_GEOLOCATION,
    CONFIG_UNIQUE_COLUMNS_MASTER,
    CONFIG_UNIQUE_COLUMNS_REFERENCE,
    CONFIG_UNIQUE_SET_COLUMNS,
    CONFIG_VALUES_DOMAIN_ALL,
    CONFIG_VALUES_DOMAIN_COVERAGE,
    CONFIG_VALUES_DOMAIN_GEOLOCATION,
    # Domain Configs
    CONFIG_VALUES_DOMAIN_MASTER,
    CONFIG_VALUES_DOMAIN_REFERENCE,
    CONFIG_VALUES_RANGE_ALL,
    CONFIG_VALUES_RANGE_COVERAGE,
    CONFIG_VALUES_RANGE_COVERAGE_ITU,
    CONFIG_VALUES_RANGE_GEOLOCATION,
    # Range Configs
    CONFIG_VALUES_RANGE_MASTER,
    CONFIG_VALUES_RANGE_REFERENCE,
    SIMILARITY_RATIO_CUTOFF,
)
from src.utils.logger import (
    ContextLoggerWithLoguruFallback,
    get_context_with_fallback_logger,
)

from .user_defined_functions import (
    get_decimal_places_udf_factory,
    h3_geo_to_h3_udf,
    is_not_within_country_check_udf_factory,
    point_110_udf,
)


# STANDARD CHECKS
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
    for column, dtype in CONFIG_DATA_TYPES:
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

    return df.withColumns(column_actions)


# CUSTOM CHECKS
# Within Country Check

azure_sas_token = settings.AZURE_SAS_TOKEN
azure_blob_container_name = settings.AZURE_BLOB_CONTAINER_NAME

DUPLICATE_SCHOOL_DISTANCE_KM = 0.1

ACCOUNT_URL = "https://saunigiga.blob.core.windows.net/"
DIRECTORY_LOCATION = "raw/geospatial-data/gadm_files/version4.1/"
container_name = azure_blob_container_name


def get_country_geometry(country_code_iso3: str):
    try:
        service = BlobServiceClient(account_url=ACCOUNT_URL, credential=azure_sas_token)
        filename = f"{country_code_iso3}.gpkg"
        file = f"{DIRECTORY_LOCATION}{filename}"
        blob_client = service.get_blob_client(container=container_name, blob=file)
        with io.BytesIO() as file_blob:
            download_stream = blob_client.download_blob()
            download_stream.readinto(file_blob)
            file_blob.seek(0)
            gdf_boundaries = gpd.read_file(file_blob)

        country_geometry = gdf_boundaries[gdf_boundaries["GID_0"] == country_code_iso3][
            "geometry"
        ][0]
    except ValueError as e:
        if str(e) == "Must be a coordinate pair or Point":
            return None
        else:
            raise e

    return country_geometry


def is_not_within_country(
    df: sql.DataFrame, country_code_iso3: str, context: OpExecutionContext = None
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Checking if not within country...")

    # boundary constants
    geometry = get_country_geometry(country_code_iso3)

    # geopy constants
    country_code_iso2 = coco.convert(names=[country_code_iso3], to="ISO2")

    is_not_within_country_check = is_not_within_country_check_udf_factory(
        country_code_iso2, country_code_iso3, geometry
    )

    return df.withColumn(
        "dq_is_not_within_country",
        is_not_within_country_check(f.col("latitude"), f.col("longitude")),
    )


def has_similar_name(df: sql.DataFrame, context: OpExecutionContext = None):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running has similar name checks...")

    name_list = df.select(f.col("school_name")).collect()
    name_list = [row["school_name"] for row in name_list]
    with_similar_name = []

    for index in range(len(name_list)):
        string_value = name_list.pop(index)
        if string_value in with_similar_name:
            name_list.insert(index, string_value)
            continue

        for name in name_list:
            if (
                SequenceMatcher(None, string_value, name).ratio()
                > SIMILARITY_RATIO_CUTOFF
            ):
                with_similar_name.append(string_value)
                with_similar_name.append(name)
                break

        name_list.insert(index, string_value)

    return df.withColumn(
        "dq_has_similar_name",
        f.when(f.col("school_name").isin(with_similar_name), 1).otherwise(0),
    )


# Decimal places tests


def precision_check(
    df: sql.DataFrame,
    config_column_list: dict[str, Any],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running precision checks...")

    column_actions = {}
    for column in config_column_list:
        precision = config_column_list[column]["min"]
        get_decimal_places = get_decimal_places_udf_factory(precision)
        column_actions[f"dq_precision-{column}"] = get_decimal_places(f.col(column))

    return df.withColumns(column_actions)


# Duplicate Sets checks


def duplicate_set_checks(
    df: sql.DataFrame, config_column_list: set[str], context: OpExecutionContext = None
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate set checks...")

    df = df.withColumn(
        "location_id",
        f.concat_ws(
            "_",
            f.col("longitude").cast("string"),
            f.col("latitude").cast("string"),
        ),
    )

    column_actions = {}
    for column_set in config_column_list:
        set_name = "_".join(column_set)
        column_actions[f"dq_duplicate_set-{set_name}"] = f.when(
            f.count("*").over(Window.partitionBy(column_set)) > 1,
            1,
        ).otherwise(0)

    df = df.withColumns(column_actions)
    return df.drop("location_id")


def duplicate_all_except_checks(
    df: sql.DataFrame, config_column_list: list[str], context: OpExecutionContext = None
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate all except checks...")

    df = df.withColumn(
        "dq_duplicate_all_except_school_code",
        f.when(
            f.count("*").over(Window.partitionBy(config_column_list)) > 1, 1
        ).otherwise(0),
    )

    return df


# Geospatial Checks


def duplicate_name_level_110_check(
    df: sql.DataFrame, context: OpExecutionContext = None
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running duplicate level within 110m checks...")

    df_columns = df.columns

    df = df.withColumn("lat_110", point_110_udf(f.col("latitude")))
    df = df.withColumn("long_110", point_110_udf(f.col("longitude")))
    window_spec1 = Window.partitionBy(
        "school_name", "education_level", "lat_110", "long_110"
    )
    df = df.withColumn(
        "dq_duplicate_name_level_within_110m_radius",
        f.when(f.count("*").over(window_spec1) > 1, 1).otherwise(0),
    )

    added_columns = ["lat_110", "long_110"]
    columns_to_drop = [col for col in added_columns if col not in df_columns]

    return df.drop(*columns_to_drop)


def similar_name_level_within_110_check(
    df: sql.DataFrame, context: OpExecutionContext = None
):
    __test_name__ = "similar name level within 110m"
    logger = get_context_with_fallback_logger(context)
    logger.info(f"Running {__test_name__} checks...")

    df_columns = df.columns

    column_actions = {
        "lat_110": point_110_udf(f.col("latitude")),
        "long_110": point_110_udf(f.col("longitude")),
    }
    df = df.withColumns(column_actions)

    window_spec2 = Window.partitionBy("education_level", "lat_110", "long_110")
    df = df.withColumn(
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        f.when(
            f.count("*").over(window_spec2) > 1,
            1,
        ).otherwise(0),
    )

    df = has_similar_name(df, context)
    df = df.withColumn(
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        f.when(
            (f.col("dq_has_similar_name") == True)  # noqa: E712
            & (f.col("dq_duplicate_similar_name_same_level_within_110m_radius") == 1),
            1,
        ).otherwise(0),
    )

    added_columns = ["lat_110", "long_110", "dq_has_similar_name"]
    columns_to_drop = [col for col in added_columns if col not in df_columns]

    return df.drop(*columns_to_drop)


def school_density_check(df: sql.DataFrame, context: OpExecutionContext = None):
    __test_name__ = "school density"
    logger = ContextLoggerWithLoguruFallback(context, __test_name__)
    logger.log.info(f"Running {__test_name__} checks...")

    column_actions = {
        "latitude": f.col("latitude").cast(FloatType()),
        "longitude": f.col("longitude").cast(FloatType()),
    }
    df = df.withColumns(column_actions)

    df = df.withColumn(
        "hex8",
        h3_geo_to_h3_udf(f.col("latitude"), f.col("longitude")),
    )

    df = df.withColumn(
        "school_density",
        f.count("school_id_giga").over(Window.partitionBy("hex8")),
    )

    df = df.withColumn(
        "dq_is_school_density_greater_than_5",
        f.when(
            f.col("school_density") > 5,
            1,
        ).otherwise(0),
    )

    return df.drop("hex8", "school_density")


# Critical Error Check


def critical_error_checks(
    df: sql.DataFrame,
    dataset_type: str,
    config_column_list: list[str],
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running critical error checks...")

    # df = duplicate_checks(df, CONFIG_UNIQUE_COLUMNS_CRITICAL)
    # df = completeness_checks(df, CONFIG_NONEMPTY_COLUMNS_CRITICAL)
    # df = range_checks(df, CONFIG_VALUES_RANGE_CRITICAL)
    # df = is_not_within_country(df, country_code_iso3)

    critial_column_dq_checks = [
        f.col(f"dq_is_null_mandatory-{column}") for column in config_column_list
    ]

    if dataset_type == "master":
        critial_column_dq_checks = [
            *critial_column_dq_checks,
            f.col("dq_duplicate-school_id_govt"),
            f.col("dq_duplicate-school_id_giga"),
            f.col("dq_is_invalid_range-latitude"),
            f.col("dq_is_invalid_range-longitude"),
            # f.col("dq_is_not_within_country"),
        ]

    df = df.withColumn(
        "dq_has_critical_error",
        f.when(
            reduce(add, critial_column_dq_checks) > 0,
            1,
        ).otherwise(0),
    )

    return df


# Coverage FB Sum is 100


def fb_percent_sum_to_100_check(df: sql.DataFrame, context: OpExecutionContext = None):
    logger = get_context_with_fallback_logger(context)
    logger.info("Running sum of percent not equal 100 checks...")

    df = df.withColumn(
        "dq_is_sum_of_percent_not_equal_100",
        f.when(
            f.col("percent_2G") + f.col("percent_3G") + f.col("percent_4G") != 100,
            1,
        ).otherwise(0),
    )
    return df


# dynamic configs
CONFIG_UNIQUE_COLUMNS = {
    "master": CONFIG_UNIQUE_COLUMNS_MASTER,
    "reference": CONFIG_UNIQUE_COLUMNS_REFERENCE,
    "geolocation": CONFIG_UNIQUE_COLUMNS_GEOLOCATION,
    "coverage": CONFIG_UNIQUE_COLUMNS_COVERAGE,
    "coverage_fb": CONFIG_UNIQUE_COLUMNS_COVERAGE_FB,
    "coverage_itu": CONFIG_UNIQUE_COLUMNS_COVERAGE_ITU,
}

CONFIG_NONEMPTY_COLUMNS = {
    "master": CONFIG_NONEMPTY_COLUMNS_MASTER,
    "reference": CONFIG_NONEMPTY_COLUMNS_REFERENCE,
    "geolocation": CONFIG_NONEMPTY_COLUMNS_GEOLOCATION,
    "coverage": CONFIG_NONEMPTY_COLUMNS_COVERAGE,
    "coverage_fb": CONFIG_NONEMPTY_COLUMNS_COVERAGE_FB,
    "coverage_itu": CONFIG_NONEMPTY_COLUMNS_COVERAGE_ITU,
}

CONFIG_VALUES_DOMAIN = {
    "master": CONFIG_VALUES_DOMAIN_MASTER,
    "reference": CONFIG_VALUES_DOMAIN_REFERENCE,
    "geolocation": CONFIG_VALUES_DOMAIN_GEOLOCATION,
    "coverage": CONFIG_VALUES_DOMAIN_COVERAGE,
}

CONFIG_VALUES_RANGE = {
    "master": CONFIG_VALUES_RANGE_MASTER,
    "reference": CONFIG_VALUES_RANGE_REFERENCE,
    "geolocation": CONFIG_VALUES_RANGE_GEOLOCATION,
    "coverage": CONFIG_VALUES_RANGE_COVERAGE,
    "coverage_itu": CONFIG_VALUES_RANGE_COVERAGE_ITU,
}

CONFIG_COLUMNS_EXCEPT_SCHOOL_ID = {
    "master": CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_MASTER,
    "geolocation": CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_GEOLOCATION,
}


def standard_checks(
    df: sql.DataFrame,
    dataset_type: str,
    context: OpExecutionContext = None,
    duplicate=True,
    completeness=True,
    domain=True,
    range_=True,
    format_=True,
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

    return df


# Outputs
def row_level_checks(
    df: sql.DataFrame,
    dataset_type: str,
    country_code_iso3: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    df = df.alias("pre_checks")

    logger = get_context_with_fallback_logger(context)
    logger.info("Starting row level checks...")

    if dataset_type in ["master", "geolocation"]:
        df = standard_checks(df, dataset_type, context)
        df = duplicate_all_except_checks(
            df, CONFIG_COLUMNS_EXCEPT_SCHOOL_ID[dataset_type], context
        )
        df = precision_check(df, CONFIG_PRECISION, context)
        # df = is_not_within_country(df, country_code_iso3, context)
        df = duplicate_set_checks(df, CONFIG_UNIQUE_SET_COLUMNS, context)
        df = duplicate_name_level_110_check(df, context)
        # df = similar_name_level_within_110_check(df, context)
        df = critical_error_checks(
            df, dataset_type, CONFIG_NONEMPTY_COLUMNS[dataset_type], context
        )
        df = school_density_check(df, context)
    elif dataset_type in ["coverage", "reference"]:
        df = standard_checks(df, dataset_type, context)
    elif dataset_type == "coverage_fb":
        df = standard_checks(df, dataset_type, context, domain=False, range_=False)
        df = fb_percent_sum_to_100_check(df, context)
    elif dataset_type == "coverage_itu":
        df = standard_checks(df, dataset_type, context, domain=False)

    df = df.alias("post_checks")
    logger.info(f"spark: {len(df.columns)=}, {df.count()=}")
    return df


def aggregate_report_spark_df(
    spark: SparkSession,
    df: sql.DataFrame,
):  # input df == row level checks results
    dq_columns = [col for col in df.columns if col.startswith("dq_")]

    df = df.select(*dq_columns)

    for column_name in df.columns:
        df = df.withColumn(column_name, f.col(column_name).cast("int"))

    # Unpivot Row Level Checks
    stack_expr = ", ".join([f"'{col.split('_', 1)[1]}', `{col}`" for col in dq_columns])
    unpivoted_df = df.selectExpr(
        f"stack({len(dq_columns)}, {stack_expr}) as (assertion, value)"
    )
    # unpivoted_df.show()

    agg_df = unpivoted_df.groupBy("assertion").agg(
        f.expr("count(CASE WHEN value = 1 THEN value END) as count_failed"),
        f.expr("count(CASE WHEN value = 0 THEN value END) as count_passed"),
        f.expr("count(value) as count_overall"),
    )

    agg_df = agg_df.withColumn(
        "percent_failed", (f.col("count_failed") / f.col("count_overall")) * 100
    )
    agg_df = agg_df.withColumn(
        "percent_passed", (f.col("count_passed") / f.col("count_overall")) * 100
    )

    # Processing for Human Readable Report
    agg_df = agg_df.withColumn("column", (f.split(f.col("assertion"), "-").getItem(1)))
    agg_df = agg_df.withColumn(
        "assertion", (f.split(f.col("assertion"), "-").getItem(0))
    )
    # agg_df.show()

    # descriptions
    configs_df = spark.createDataFrame(CONFIG_DATA_QUALITY_CHECKS_DESCRIPTIONS)
    # configs_df.show(truncate=False)

    # Range
    r_rows = [
        (key, value["min"], value.get("max"))
        for key, value in CONFIG_VALUES_RANGE_ALL.items()
    ]
    range_schema = StructType(
        [
            StructField("column", StringType(), True),
            StructField("min", IntegerType(), True),
            StructField("max", IntegerType(), True),
        ]
    )
    range_df = spark.createDataFrame(r_rows, schema=range_schema)
    # range_df.show(truncate=False)

    # Domain
    d_rows = list(CONFIG_VALUES_DOMAIN_ALL.items())
    domain_schema = StructType(
        [
            StructField("column", StringType(), True),
            StructField("set", ArrayType(StringType(), True), True),
        ]
    )
    domain_df = spark.createDataFrame(d_rows, schema=domain_schema)
    # domain_df.show(truncate=False)

    # Precision
    p_rows = [(key, value["min"]) for key, value in CONFIG_PRECISION.items()]
    precision_schema = StructType(
        [
            StructField("column", StringType(), True),
            StructField("precision", IntegerType(), True),
        ]
    )
    precision_df = spark.createDataFrame(p_rows, schema=precision_schema)
    # precision_df.show()

    # Report Construction
    report = agg_df.join(configs_df, "assertion", "left")
    report = report.join(range_df, "column", "left")
    report = report.join(domain_df, "column", "left")
    report = report.join(precision_df, "column", "left")
    report = report.withColumn(
        "description",
        f.when(f.col("column").isNull(), f.col("description")).otherwise(
            f.regexp_replace("description", "\\{\\}", f.col("column"))
        ),
    )
    report = report.withColumn(
        "description",
        f.when(f.col("min").isNull(), f.col("description")).otherwise(
            f.regexp_replace("description", "\\{min\\}", f.col("min"))
        ),
    )
    report = report.withColumn(
        "description",
        f.when(f.col("max").isNull(), f.col("description")).otherwise(
            f.regexp_replace("description", "\\{max\\}", f.col("max"))
        ),
    )
    report = report.withColumn(
        "description",
        f.when(f.col("set").isNull(), f.col("description")).otherwise(
            f.regexp_replace(
                "description", "\\{set\\}", f.array_join(f.col("set"), ", ")
            )
        ),
    )
    report = report.withColumn(
        "description",
        f.when(f.col("precision").isNull(), f.col("description")).otherwise(
            f.regexp_replace("description", "\\{precision\\}", f.col("precision"))
        ),
    )
    report = report.select(
        "type",
        "assertion",
        "column",
        "description",
        "count_failed",
        "count_passed",
        "count_overall",
        "percent_failed",
        "percent_passed",
    )

    return report


def aggregate_report_json(
    df_aggregated, df_bronze
):  # input: df_aggregated = aggregated row level checks, df_bronze = bronze df
    # Summary Report
    rows_count = df_bronze.count()
    columns_count = len(df_bronze.columns)
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    # Summary Dictionary
    summary = {
        "summary": {
            "rows": rows_count,
            "columns": columns_count,
            "timestamp": timestamp,
        }
    }

    # Initialize an empty dictionary for the transformed data
    json_array = df_aggregated.toJSON().collect()
    transformed_data = {"summary": summary}

    # Iterate through each JSON line
    for line in json_array:
        # Parse the JSON line into a dictionary
        data = json.loads(line)

        # Extract the 'type' value to use as a key
        key = data.pop("type")

        # Append the rest of the dictionary to the list associated with the 'type' key
        if key not in transformed_data:
            transformed_data[key] = [data]
        else:
            transformed_data[key].append(data)

    json_dict = json.dumps(transformed_data, indent=4)

    return json_dict


def dq_passed_rows(df: sql.DataFrame, dataset_type: str):
    if dataset_type in ["master", "reference"]:
        schema_name = f"school_{dataset_type}"
        schema: BaseSchema = getattr(src.schemas, schema_name)
        columns = [col.name for col in schema.columns]
    else:
        columns = [col for col in df.columns if not col.startswith("dq_")]

    if dataset_type in ["master", "geolocation"]:
        df = df.filter(df.dq_has_critical_error == 0)
        df = df.select(*columns)
    else:
        df = df.filter(
            (df["dq_duplicate-school_id_giga"] == 0)
            & (df["dq_is_null_mandatory-school_id_giga"] == 0)
            & (df["dq_is_null_mandatory-education_level_govt"] == 0)
            & (df["dq_is_null_mandatory-school_id_govt_type"] == 0)
        )
        df = df.select(*columns)
    return df


def dq_failed_rows(df: sql.DataFrame, dataset_type: str):
    if dataset_type in ["master", "geolocation"]:
        df = df.filter(df.dq_has_critical_error == 1)
    else:
        df = df.filter(
            (df["dq_duplicate-school_id_giga"] == 1)
            | (df["dq_is_null_mandatory-school_id_giga"] == 1)
            | (df["dq_is_null_mandatory-education_level_govt"] == 1)
            | (df["dq_is_null_mandatory-school_id_govt_type"] == 1)
        )
    return df


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    spark = get_spark_session()
    #
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/GHA_school_geolocation_coverage_master.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    df_bronze = spark.read.csv(file_url, header=True)
    df_bronze = df_bronze.sort("school_name").limit(1000)
    df_bronze = df_bronze.withColumnRenamed("school_id_gov", "school_id_govt")
    df_bronze = df_bronze.withColumnRenamed("num_classroom", "num_classrooms")
    # df = domain_checks(df_bronze, CONFIG_VALUES_DOMAIN_MASTER)
    df_test = has_similar_name(df_bronze)
    df_test.show()
    # df_bronze = df_bronze.withColumn("test", f.lower(f.col("admin2_id_giga")))

    # row_level_checks(df, dataset_type, country_code_iso3)
    # df = row_level_checks(
    #     df_bronze, "master", "GHA")
    # dataset plugged in should conform to updated schema! rename if necessary
    # df.show()
    # df = df.withColumn("dq_has_critical_error", f.lit(1))

    # df = dq_passed_rows(df, "coverage")
    # df = dq_passed_rows(df, "coverage")
    # df = aggregate_report_sparkdf(df)
    # df.show()
    # df.show()

    # _json = aggregate_report_json(df, df_bronze)
    # print(_json)
