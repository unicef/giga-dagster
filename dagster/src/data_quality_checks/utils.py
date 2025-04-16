from datetime import UTC, datetime

from jinja2 import BaseLoader, Environment
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

from azure.storage.blob import BlobServiceClient
from dagster import OpExecutionContext
from src.constants import UploadMode
from src.data_quality_checks.column_relation import column_relation_checks
from src.data_quality_checks.config import (
    CONFIG_COLUMNS_EXCEPT_SCHOOL_ID,
    CONFIG_NONEMPTY_COLUMNS,
)
from src.data_quality_checks.coverage import fb_percent_sum_to_100_check
from src.data_quality_checks.create_update import (
    create_checks,
    update_checks,
)
from src.data_quality_checks.critical import critical_error_checks
from src.data_quality_checks.duplicates import (
    duplicate_all_except_checks,
    duplicate_set_checks,
)
from src.data_quality_checks.geography import is_not_within_country
from src.data_quality_checks.geometry import (
    duplicate_name_level_110_check,
    school_density_check,
    similar_name_level_within_110_check,
)
from src.data_quality_checks.precision import precision_check
from src.data_quality_checks.standard import standard_checks
from src.settings import settings
from src.spark.config_expectations import config
from src.utils.logger import get_context_with_fallback_logger
from src.utils.schema import get_schema_columns


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
        f"stack({len(dq_columns)}, {stack_expr}) as (assertion, value)",
    )
    # unpivoted_df.show()

    agg_df = unpivoted_df.groupBy("assertion").agg(
        f.expr("count(CASE WHEN value = 1 THEN value END) as count_failed"),
        f.expr("count(CASE WHEN value = 0 THEN value END) as count_passed"),
        f.expr("count(value) as count_overall"),
    )

    agg_df = agg_df.withColumn(
        "percent_failed",
        (f.col("count_failed") / f.col("count_overall")) * 100,
    )
    agg_df = agg_df.withColumn(
        "percent_passed",
        (f.col("count_passed") / f.col("count_overall")) * 100,
    )
    agg_df = agg_df.withColumn(
        "dq_remarks",
        f.when(f.col("count_failed") == 0, "pass").otherwise("fail"),
    )

    # Processing for Human Readable Report
    agg_df = agg_df.withColumn("column", (f.split(f.col("assertion"), "-").getItem(1)))
    agg_df = agg_df.withColumn(
        "assertion",
        (f.split(f.col("assertion"), "-").getItem(0)),
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
        ],
    )
    range_df = spark.createDataFrame(r_rows, schema=range_schema)
    # range_df.show(truncate=False)

    # Domain
    d_rows = list(config.VALUES_DOMAIN_ALL.items())
    domain_schema = StructType(
        [
            StructField("column", StringType(), True),
            StructField("set", ArrayType(StringType(), True), True),
        ],
    )
    domain_df = spark.createDataFrame(d_rows, schema=domain_schema)
    # domain_df.show(truncate=False)

    # Precision
    p_rows = [(key, value["min"]) for key, value in config.PRECISION.items()]
    precision_schema = StructType(
        [
            StructField("column", StringType(), True),
            StructField("precision", IntegerType(), True),
        ],
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
            f.regexp_replace("description", "\\{\\}", f.col("column")),
        ),
    )
    report = report.withColumn(
        "description",
        f.when(f.col("min").isNull(), f.col("description")).otherwise(
            f.regexp_replace("description", "\\{min\\}", f.col("min")),
        ),
    )
    report = report.withColumn(
        "description",
        f.when(f.col("max").isNull(), f.col("description")).otherwise(
            f.regexp_replace("description", "\\{max\\}", f.col("max")),
        ),
    )
    report = report.withColumn(
        "description",
        f.when(f.col("set").isNull(), f.col("description")).otherwise(
            f.regexp_replace(
                "description",
                "\\{set\\}",
                f.array_join(f.col("set"), ", "),
            ),
        ),
    )
    report = report.withColumn(
        "description",
        f.when(f.col("precision").isNull(), f.col("description")).otherwise(
            f.regexp_replace("description", "\\{precision\\}", f.col("precision")),
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
        "dq_remarks",
    )
    report = report.withColumn("column", f.coalesce(f.col("column"), f.lit("")))

    return report


def aggregate_report_json(
    df_aggregated: sql.DataFrame,
    df_bronze: sql.DataFrame,
    df_data_quality_checks: sql.DataFrame,
):  # input: df_aggregated = aggregated row level checks, df_bronze = bronze df
    # Summary Report
    counts = df_data_quality_checks.select(
        f.count("*").alias("rows_count"),
        f.count(f.when(f.col("dq_has_critical_error") == 0, 1)).alias("rows_passed"),
        f.count(f.when(f.col("dq_has_critical_error") == 1, 1)).alias("rows_failed"),
    ).first()
    rows_count = counts.rows_count
    rows_passed = counts.rows_passed
    rows_failed = counts.rows_failed

    columns_count = len(
        [
            col
            for col in df_bronze.columns
            if not col.startswith("dq_")
            or not col.startswith("_")
            or col != "failure_reason"
        ]
    )
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    # Summary Dictionary
    summary = {
        "rows": rows_count,
        "rows_passed": rows_passed,
        "rows_failed": rows_failed,
        "columns": columns_count,
        "timestamp": timestamp,
    }

    # Initialize an empty dictionary for the transformed data
    agg_array = df_aggregated.toPandas().to_dict(orient="records")
    transformed_data = {"summary": summary}

    # Iterate through each JSON line
    for agg in agg_array:
        # Extract the 'type' value to use as a key
        key = agg.pop("type")

        # Append the rest of the dictionary to the list associated with the 'type' key
        if key not in transformed_data:
            transformed_data[key] = [agg]
        else:
            transformed_data[key].append(agg)

    return transformed_data


def aggregate_report_statistics(df: sql.DataFrame, upload_details: dict):
    # add necessary columns
    df = df.withColumn(
        "dq_missing_location",
        f.when(
            (f.col("dq_is_null_optional-latitude") == 1)
            | (f.col("dq_is_null_optional-latitude") == 1),
            1,
        ).otherwise(0),
    )

    df = df.withColumn(
        "dq_is_null_connectivity_type_when_connectivity_govt",
        f.when(
            (f.col("dq_is_null_optional-connectivity_type_govt") == 1)
            & (f.col("dq_is_null_optional-connectivity_govt") == 0),
            1,
        ).otherwise(0),
    )

    count_schools_raw_file = df.count()

    dq_report_columns = [
        "dq_duplicate-school_id_govt",
        "dq_duplicate_all_except_school_code",
        "dq_duplicate_name_level_within_110m_radius",
        "dq_duplicate_set-education_level_location_id",
        "dq_duplicate_set-school_id_govt_school_name_education_level_location_id",
        "dq_duplicate_set-school_name_education_level_location_id",
        "dq_duplicate_similar_name_same_level_within_110m_radius",
        "dq_has_critical_error",
        "dq_is_not_within_country",
        "dq_is_null_connectivity_type_when_connectivity_govt",
        "dq_is_null_mandatory-school_id_govt",
        "dq_is_null_optional-computer_availability",
        "dq_is_null_optional-connectivity_govt",
        "dq_is_null_optional-education_level_govt",
        "dq_is_null_optional-school_name",
        "dq_is_school_density_greater_than_5",
        "dq_missing_location",
        "dq_precision-latitude",
        "dq_precision-longitude",
    ]

    df_report = df.select(*dq_report_columns)

    for column_name in df_report.columns:
        df_report = df_report.withColumn(column_name, f.col(column_name).cast("int"))

    dq_duplicate_columns = [
        col for col in dq_report_columns if col.startswith("dq_duplicate")
    ]
    check_duplicate_columns = [
        f.col(duplicate_col) == 1 for duplicate_col in dq_duplicate_columns
    ]

    df_report = df_report.withColumn(
        "dq_suspected_duplicate",
        f.when(f.greatest(check_duplicate_columns), 1).otherwise(0),
    )

    stack_expr = ", ".join(
        [f"'{col.split('_', 1)[1]}', `{col}`" for col in dq_report_columns]
    )

    unpivoted_df = df_report.selectExpr(
        f"stack({len(dq_report_columns)}, {stack_expr}) as (assertion, value)",
    )

    agg_df = unpivoted_df.groupBy("assertion").agg(
        f.expr("count(CASE WHEN value = 1 THEN value END) as count_schools")
    )

    agg_df_pd = agg_df.toPandas()
    agg_df_pd.set_index("assertion", inplace=True)

    # extract required statistics
    count_unique_schools_ids = df.select("school_id_govt").distinct().count()
    stats = {
        "country": upload_details["country"],
        "file_name": upload_details["file_name"],
        "count_schools_raw_file": count_schools_raw_file,
        "count_schools_dropped": agg_df_pd.at["has_critical_error", "count_schools"],
        "count_schools_passed": count_schools_raw_file
        - agg_df_pd.at["has_critical_error", "count_schools"],
        "count_schools_no_location": agg_df_pd.at["missing_location", "count_schools"],
        "count_schools_outside_country": agg_df_pd.at[
            "is_not_within_country", "count_schools"
        ],
        "count_unique_school_id_govt": count_unique_schools_ids,
        "percent_unique_school_ids": round(
            (100 * count_unique_schools_ids / count_schools_raw_file), 2
        ),
        "count_null_school_id_govt": agg_df_pd.at[
            "is_null_mandatory-school_id_govt", "count_schools"
        ],
        "count_duplicate_school_id": agg_df_pd.at[
            "duplicate-school_id_govt", "count_schools"
        ],
        "count_null_school_name": agg_df_pd.at[
            "is_null_optional-school_name", "count_schools"
        ],
        "count_null_education_level_govt": agg_df_pd.at[
            "is_null_optional-education_level_govt", "count_schools"
        ],
        "count_null_connectivity_govt": agg_df_pd.at[
            "is_null_optional-connectivity_govt", "count_schools"
        ],
        "count_null_connectivity_type_govt_when_connectivity": agg_df_pd.at[
            "is_null_connectivity_type_when_connectivity_govt", "count_schools"
        ],
        "count_null_computer_availability": agg_df_pd.at[
            "is_null_optional-computer_availability", "count_schools"
        ],
        "count_school_density_greater_than_5": agg_df_pd.at[
            "is_school_density_greater_than_5", "count_schools"
        ],
        "count_suspected_duplicate": agg_df_pd.at[
            "suspected_duplicate", "count_schools"
        ],
        "count_duplicate_all_except_school_code": agg_df_pd.at[
            "duplicate_all_except_school_code", "count_schools"
        ],
        "count_duplicate_school_id_govt_school_name_education_level_location_id": agg_df_pd.at[
            "duplicate_set-school_id_govt_school_name_education_level_location_id",
            "count_schools",
        ],
        "count_duplicate_school_name_education_level_location_id": agg_df_pd.at[
            "duplicate_set-school_name_education_level_location_id", "count_schools"
        ],
        "count_duplicate_education_level_location_id": agg_df_pd.at[
            "duplicate_set-education_level_location_id", "count_schools"
        ],
        "count_duplicate_name_level_within_110m_radius": agg_df_pd.at[
            "duplicate_name_level_within_110m_radius", "count_schools"
        ],
        "count_duplicate_similar_name_same_level_within_110m_radius": agg_df_pd.at[
            "duplicate_similar_name_same_level_within_110m_radius", "count_schools"
        ],
        "count_precision_latitude": agg_df_pd.at["precision-latitude", "count_schools"],
        "count_precision_longitude": agg_df_pd.at[
            "precision-longitude", "count_schools"
        ],
    }

    # counts in education_level_govt
    education_level_counts = (
        df.groupBy("education_level_govt").count().toPandas().fillna("Unknown")
    )
    if not (
        education_level_counts.shape[0] == 1
        and education_level_counts.at[
            (education_level_counts["education_level_govt"] == "Unknown").index[0],
            "count",
        ]
        == count_schools_raw_file
    ):
        education_level_counts.columns = [None] * len(education_level_counts.columns)
        education_level_counts_string = education_level_counts.to_string(index=False)
        education_level_counts_string = education_level_counts_string.split("\n", 1)[1]
        stats["education_level_govt_counts"] = education_level_counts_string

    # counts in education_level_govt
    connectivity_govt_counts = (
        df.groupBy("connectivity_govt").count().toPandas().fillna("Unknown")
    )
    if not (
        connectivity_govt_counts.shape[0] == 1
        and connectivity_govt_counts.at[
            (connectivity_govt_counts["connectivity_govt"] == "Unknown").index[0],
            "count",
        ]
        == count_schools_raw_file
    ):
        connectivity_govt_counts.columns = [None] * len(education_level_counts.columns)
        connectivity_govt_counts_string = connectivity_govt_counts.to_string(
            index=False
        )
        connectivity_govt_counts_string = connectivity_govt_counts_string.split(
            "\n", 1
        )[1]
        stats["connectivity_govt_counts"] = connectivity_govt_counts_string

    # counts in connectivity_type_govt
    connectivity_type_counts = (
        df.groupBy("connectivity_type_govt").count().toPandas().fillna("Unknown")
    )
    if not (
        connectivity_type_counts.shape[0] == 1
        and connectivity_type_counts.at[
            (connectivity_type_counts["connectivity_type_govt"] == "Unknown").index[0],
            "count",
        ]
        == count_schools_raw_file
    ):
        connectivity_type_counts.columns = [None] * len(
            connectivity_type_counts.columns
        )
        connectivity_type_counts_string = connectivity_type_counts.to_string(
            index=False
        )
        connectivity_type_counts_string = connectivity_type_counts_string.split(
            "\n", 1
        )[1]
        stats["connectivity_type_govt_counts"] = connectivity_type_counts_string

    report_template_string = get_report_template()
    report_template = Environment(loader=BaseLoader()).from_string(
        report_template_string
    )
    data_quality_report = report_template.render(**stats)

    return data_quality_report


def get_report_template() -> str:
    account_url = f"https://{settings.AZURE_DFS_SAS_HOST}"
    azure_sas_token = settings.AZURE_SAS_TOKEN
    container_name = settings.AZURE_BLOB_CONTAINER_NAME

    service = BlobServiceClient(account_url=account_url, credential=azure_sas_token)
    filename = "templates/data_quality/data_quality_report_template.txt"
    blob_client = service.get_blob_client(container=container_name, blob=filename)
    data = blob_client.download_blob(encoding="UTF-8").readall()
    return data


def dq_split_passed_rows(df: sql.DataFrame, dataset_type: str):
    if dataset_type in ["master", "reference"]:
        schema_name = f"school_{dataset_type}"
        schema_columns = get_schema_columns(df.sparkSession, schema_name)
        columns = [col.name for col in schema_columns]
    else:
        columns = [
            col
            for col in df.columns
            if not (col.startswith("dq_") or col == "failure_reason")
        ]

    df = df.filter(df.dq_has_critical_error == 0)
    df = df.select(*columns)
    return df


def dq_split_failed_rows(df: sql.DataFrame, dataset_type: str):
    df = df.filter(df.dq_has_critical_error == 1)
    return df


def row_level_checks(
    df: sql.DataFrame,
    dataset_type: str,
    _country_code_iso3: str,
    silver: sql.DataFrame = None,
    mode=None,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    logger = get_context_with_fallback_logger(context)
    logger.info("Starting row level checks...")

    if dataset_type == "master":
        df = is_not_within_country(df, _country_code_iso3, context)
        df = similar_name_level_within_110_check(df, context)
        df = school_density_check(df, context)
        df = standard_checks(df, dataset_type, context)
        df = duplicate_all_except_checks(
            df,
            CONFIG_COLUMNS_EXCEPT_SCHOOL_ID[dataset_type],
            context,
        )
        df = precision_check(df, config.PRECISION, context)
        df = duplicate_set_checks(df, config.UNIQUE_SET_COLUMNS, context)
        df = duplicate_name_level_110_check(df, context)
        df = column_relation_checks(df, dataset_type, context)
        df = critical_error_checks(
            df,
            dataset_type,
            CONFIG_NONEMPTY_COLUMNS[dataset_type],
            context,
        )
    elif dataset_type == "geolocation":
        if mode == UploadMode.CREATE.value:
            df = create_checks(bronze=df, silver=silver, context=context)
        elif mode == UploadMode.UPDATE.value:
            df = update_checks(bronze=df, silver=silver, context=context)

        df = is_not_within_country(df, _country_code_iso3, context)
        df = similar_name_level_within_110_check(df, context)
        df = school_density_check(df, context)
        df = standard_checks(df, dataset_type, context)
        df = duplicate_all_except_checks(
            df,
            CONFIG_COLUMNS_EXCEPT_SCHOOL_ID[dataset_type],
            context,
        )
        df = precision_check(df, config.PRECISION, context)
        df = duplicate_set_checks(df, config.UNIQUE_SET_COLUMNS, context)
        df = duplicate_name_level_110_check(df, context)
        df = critical_error_checks(
            df,
            dataset_type,
            CONFIG_NONEMPTY_COLUMNS[dataset_type],
            mode,
            context,
        )
        df = column_relation_checks(df, dataset_type, context)
    elif dataset_type == "reference":
        df = standard_checks(df, dataset_type, context)
        df = critical_error_checks(
            df,
            dataset_type,
            CONFIG_NONEMPTY_COLUMNS[dataset_type],
            context,
        )
    elif dataset_type in ["coverage", "coverage_itu"]:
        df = standard_checks(df, dataset_type, context)
        df = column_relation_checks(df, dataset_type, context)
        df = critical_error_checks(
            df,
            dataset_type,
            CONFIG_NONEMPTY_COLUMNS[dataset_type],
            context,
        )
    elif dataset_type == "coverage_fb":
        df = standard_checks(df, dataset_type, context, domain=False, range_=False)
        df = fb_percent_sum_to_100_check(df, context)
        df = column_relation_checks(df, dataset_type, context)
        df = critical_error_checks(
            df,
            dataset_type,
            CONFIG_NONEMPTY_COLUMNS[dataset_type],
            context,
        )
    elif dataset_type == "qos":
        df = standard_checks(df, dataset_type, context, domain=False, range_=False)
        df = critical_error_checks(
            df,
            dataset_type,
            CONFIG_NONEMPTY_COLUMNS[dataset_type],
            context,
        )
    return df


def extract_school_id_govt_duplicates(df: sql.DataFrame):
    window = w.Window.partitionBy("school_id_govt").orderBy(f.lit(1))

    df = df.withColumn("row_num", f.row_number().over(window))

    return df


if __name__ == "__main__":
    from src.settings import settings
    from src.utils.spark import get_spark_session
    # from src.spark.transform_functions import create_giga_school_id

    spark = get_spark_session()
    #
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/bronze/school-geolocation-data/BLZ_school-geolocation_gov_20230207.csv"
    # file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/GIN_school_geolocation_coverage_master.csv"
    # file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/GIN_master_reference.csv"
    # file_url_master = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/master/BRA_school_geolocation_coverage_master.csv"
    # file_url_reference = f"{settings.AZURE_BLOB_CONNECTION_URI}/updated_master_schema/reference/BLZ_master_reference.csv"
    file_url_qos = (
        f"{settings.AZURE_BLOB_CONNECTION_URI}/gold/qos/BRA/2024-03-07_04-10-02.csv"
    )
    # file_url = f"{settings.AZURE_BLOB_CONNECTION_URI}/adls-testing-raw/_test_BLZ_RAW.csv"
    # master = spark.read.csv(file_url_master, header=True)
    # reference = spark.read.csv(file_url_reference, header=True)
    qos = spark.read.csv(file_url_qos, header=True)
    # df_bronze = master.join(reference, how="left", on="school_id_giga")
    # df_bronze = spark.read.csv(file_url, header=True)
    # df_bronze.show()
    # print(df_bronze.count())
    # df_bronze = df_bronze.sort("school_name").limit(30)
    # df_bronze = df_bronze.withColumnRenamed("school_id_gov", "school_id_govt")
    # df_bronze = df_bronze.withColumnRenamed("num_classroom", "num_classrooms")
    # df_bronze.show()
    # df = create_giga_school_id(df_bronze)
    # df.show()
    qos.show()
    # master = master.filter(master["admin1"] == "São Paulo")
    # print(master.count(), len(master.columns))
    # master.show()
    # master = master.filter(~(f.col("latitude").like("%Sr%")) & ~(f.col("longitude").like("%Sr%")))
    # filtered_df.show()

    # # master
    # df = row_level_checks(master, "master", "BRA")
    # df.show()
    # # df.explain()

    # df = aggregate_report_spark_df(spark=spark, df=df)
    # df.show()

    # _json = aggregate_report_json(df, master)
    # print(_json)

    # # ref
    # df = row_level_checks(reference, "reference", "BLZ")
    # df.show()

    # df = aggregate_report_spark_df(spark=spark, df=df)
    # df.show()

    # _json = aggregate_report_json(df, reference)
    # print(_json)

    # # qos
    # df = row_level_checks(qos, "qos", "BRA")
    # df.show()

    # df = aggregate_report_spark_df(spark=spark, df=df)
    # df.show()

    # _json = aggregate_report_json(df, qos)
    # print(_json)

    # pas = dq_split_passed_rows(df, "qos")
    # fail = dq_split_failed_rows(df, "qos")

    # print(pas.count())
    # print(fail.count())
    # pas.show()
    # fail.show()
    # df_bronze = df_bronze.withColumn("connectivity_RT", f.lit("yes"))
    # df_bronze = df_bronze.select(*["connectivity", "connectivity_RT", "connectivity_govt", "download_speed_contracted", "connectivity_RT_datasource","connectivity_RT_ingestion_timestamp"])
    # df_bronze = df_bronze.select(*["connectivity_govt", "connectivity_govt_ingestion_timestamp"])
    # df_bronze = df_bronze.select(*["nearest_NR_id", "nearest_NR_distance","nearest_LTE_id", "nearest_LTE_distance","nearest_UMTS_id", "nearest_UMTS_distance","nearest_GSM_id", "nearest_GSM_distance"])
    # df_bronze = df_bronze.select(*["electricity_availability", "electricity_type"])
    # df_bronze.show()
    # df = standard_checks(df_bronze, 'master')
    # df_bronze = df_bronze.withColumn("school_id_giga", f.lit("9663bb61-6ad9-3d91-9a16-90e8c40448142"))
    # df = format_validation_checks(df_bronze)
    # df = column_relation_checks(df_bronze, 'coverage')
    # transforms = {}
    # transforms["dq_column_relation_checks-connectivity_connectivity_RT_connectivity_govt_download_speed_contracted"] = f.when(
    #             (f.lower(f.col("connectivity")) == "yes") & (
    #                 (f.lower(f.col("connectivity_RT")) == "yes") |
    #                 (f.lower(f.col("connectivity_govt")) == "yes") |
    #                 (f.col("download_speed_contracted").isNotNull())
    #                 ), 0,
    #         ).when(
    #             (f.lower(f.col("connectivity")) == "no") & (
    #                 ((f.lower(f.col("connectivity_RT")) == "no") | f.col("connectivity_RT").isNull()) &
    #                 ((f.lower(f.col("connectivity_govt")) == "no") | f.col("connectivity_govt").isNull()) &
    #                 (f.col("download_speed_contracted").isNull())
    #                 ), 0,
    #         ).otherwise(1)
    # print(transforms)
    # df = df_bronze.withColumns(transforms)
    # df.show()
    # df_bronze = df_bronze.withColumn("school_id_govt", f.lit("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Donec elementum dignissim magna, eu efficitur libero congue sit amet. Morbi posuere, quam ac convallis laoreet, ipsum elit condimentum arcu, nec sollicitudin lorem odio id nunc. Nulla facilisi. Quisque ut efficitur nisi. Vestibulum bibendum posuere elit ac vestibulum. Nullam ultrices magna nec arcu ullamcorper, a luctus eros volutpat. Proin vel libero vitae velit feugiat malesuada nec ut felis. In hac habitasse platea dictumst. Fusce euismod vestibulum lorem, ac venenatis sapien efficitur non. Sed tempor nunc sit amet velit malesuada, quis bibendum odio dictum."))
    # df = standard_checks(df_bronze, 'master')
    # df.distinct().show()

    # column_pairs = {
    #     ("nearest_NR_id", "nearest_NR_distance"),
    #     ("nearest_LTE_id", "nearest_LTE_distance"),
    #     ("nearest_UMTS_id", "nearest_UMTS_distance"),
    #     ("nearest_GSM_id", "nearest_GSM_distance"),
    # }
    # for id, distance in column_pairs:
    #     print(id, distance)

    # # df = dq_passed_rows(df, "coverage")
    # # df = dq_passed_rows(df, "coverage")
    # df.orderBy("column").show()
