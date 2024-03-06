import json
from datetime import UTC, datetime

from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
    window as w, 
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import src.schemas
from dagster import OpExecutionContext
from src.data_quality_checks.config import (
    CONFIG_COLUMNS_EXCEPT_SCHOOL_ID,
    CONFIG_NONEMPTY_COLUMNS,
)
from src.data_quality_checks.coverage import fb_percent_sum_to_100_check
from src.data_quality_checks.critical import critical_error_checks
from src.data_quality_checks.duplicates import (
    duplicate_all_except_checks,
    duplicate_set_checks,
)
from src.data_quality_checks.geometry import (
    duplicate_name_level_110_check,
    school_density_check,
)
from src.data_quality_checks.precision import precision_check
from src.data_quality_checks.standard import standard_checks, format_validation_checks
from src.schemas import BaseSchema
from src.spark.config_expectations import config
from src.utils.logger import get_context_with_fallback_logger


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
    configs_df = spark.createDataFrame(config.DATA_QUALITY_CHECKS_DESCRIPTIONS)
    # configs_df.show(truncate=False)

    # Range
    r_rows = [
        (key, value["min"], value.get("max"))
        for key, value in config.VALUES_RANGE_ALL.items()
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
    d_rows = list(config.VALUES_DOMAIN_ALL.items())
    domain_schema = StructType(
        [
            StructField("column", StringType(), True),
            StructField("set", ArrayType(StringType(), True), True),
        ]
    )
    domain_df = spark.createDataFrame(d_rows, schema=domain_schema)
    # domain_df.show(truncate=False)

    # Precision
    p_rows = [(key, value["min"]) for key, value in config.PRECISION.items()]
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
    report = report.withColumn("column", f.coalesce(f.col("column"), f.lit("")))

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

    return transformed_data


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


def row_level_checks(
    df: sql.DataFrame,
    dataset_type: str,
    country_code_iso3: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    logger = get_context_with_fallback_logger(context)
    logger.info("Starting row level checks...")

    if dataset_type in ["master", "geolocation"]:
        df = standard_checks(df, dataset_type, context)
        df = duplicate_all_except_checks(
            df, CONFIG_COLUMNS_EXCEPT_SCHOOL_ID[dataset_type], context
        )
        df = precision_check(df, config.PRECISION, context)
        # df = is_not_within_country(df, country_code_iso3, context)
        df = duplicate_set_checks(df, config.UNIQUE_SET_COLUMNS, context)
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

    return df


def extract_school_id_govt_duplicates(df: sql.DataFrame):
    window = w.Window.partitionBy("school_id_govt").orderBy(f.lit(1)) 

    df = df.withColumn("row_num", f.row_number().over(window))

    ## outputs
    # df_duplicates = df.where(df.row_num != 1)
    # df_duplicates = df_duplicates.drop("row_num")

    # df_deduplicated = df.where(df.row_num == 1)
    # df_deduplicated = df_deduplicated.drop("row_num")

    return df

if __name__ == "__main__":
    from src.utils.spark import get_spark_session
    from src.settings import settings

    spark = get_spark_session()
    #
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/GIN_school_geolocation_coverage_master.csv"
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    df_bronze = spark.read.csv(file_url, header=True)
    print(df_bronze.count())
    df_bronze = df_bronze.sort("school_name").limit(100)
    df_bronze = df_bronze.withColumnRenamed("school_id_gov", "school_id_govt")
    df_bronze = df_bronze.withColumnRenamed("num_classroom", "num_classrooms")
    df_bronze.show()
    # df = standard_checks(df_bronze, 'master')
    df_bronze = df_bronze.withColumn("school_id_giga", f.lit("9663bb61-6ad9-3d91-9a16-90e8c40448142"))
    df = format_validation_checks(df_bronze)
    df.show()
    # df = domain_checks(df_bronze, VALUES_DOMAIN_MASTER)
    # df_test = has_similar_name(df_bronze)

    # df_duplicates, df_deduplicated = extract_school_id_govt_duplicates(df_bronze)

    # df_duplicates.where(df_duplicates.school_id_govt == '11514002').show()
    # df_deduplicated.where(df_deduplicated.school_id_govt == '11514002').show()
    # df_duplicates.show()
    # df_deduplicated.show()

    
    # df_bronze.groupBy("school_id_govt").agg(f.count("*").alias("count")).orderBy("count", ascending=False).show()
    # window = w.Window.partitionBy("school_id_govt").orderBy(f.lit(1)) 
    # df_bronze = df_bronze.withColumn("test", f.row_number().over(window))
    # df_bronze.orderBy("test", ascending=False).show()
    # df_dups = df_bronze.where(df_bronze.test != 1)
    # df_dups.orderBy("school_id_govt").show(100000)


    # df_bronze = df_bronze.withColumn("test", f.lower(f.col("admin2_id_giga")))

    # # row_level_checks(df, dataset_type, country_code_iso3)
    # df = row_level_checks(
    #     df_bronze, "master", "GIN")
    # # dataset plugged in should conform to updated schema! rename if necessary
    # df.show()
    # df = df.withColumn("dq_has_critical_error", f.lit(1))

    # # df = dq_passed_rows(df, "coverage")
    # # df = dq_passed_rows(df, "coverage")
    df = aggregate_report_spark_df(spark=spark, df=df)
    # df.orderBy("column").show()
    df.show()

    _json = aggregate_report_json(df, df_bronze)
    print(_json)
