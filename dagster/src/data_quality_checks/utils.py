from datetime import UTC, datetime
from typing import Any

import pandas as pd
from jinja2 import BaseLoader, Environment
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
    window as w,
)
from pyspark.sql.types import IntegerType, MapType, StringType

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
from src.data_quality_checks.dq_context import DQContext, DQMode
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
from src.utils.adls import get_blob_service_client
from src.utils.logger import get_context_with_fallback_logger
from src.utils.nocodb.get_nocodb_data import (
    get_nocodb_table_as_pandas_dataframe,
    get_nocodb_table_id_from_name,
)
from src.utils.schema import get_schema_columns


def aggregate_report_spark_df(
    spark: SparkSession,
    df: sql.DataFrame,
):  # input df == row level checks results
    # Geolocation DQ results store individual checks in a single dq_results map column.
    # Other pipelines (master, coverage, connectivity) still use flat dq_* columns.
    # Detect which format is present and unify into (check_key, value) rows.
    if "dq_results" in df.columns:
        # Map path: explode map<string, int> + merge dq_has_critical_error
        all_checks_map = f.map_concat(
            f.col("dq_results"),
            f.create_map(
                f.lit("has_critical_error"),
                f.col("dq_has_critical_error").cast("int"),
            ),
        )
        unpivoted_df = df.select(f.explode(all_checks_map).alias("check_key", "value"))
    else:
        # Flat path: stack all dq_* columns into (check_key, value) rows
        dq_columns = [col for col in df.columns if col.startswith("dq_")]
        df_flat = df.select(*dq_columns)
        for col_name in dq_columns:
            df_flat = df_flat.withColumn(col_name, f.col(col_name).cast("int"))
        stack_expr = ", ".join(
            [f"'{col.split('_', 1)[1]}', `{col}`" for col in dq_columns]
        )
        unpivoted_df = df_flat.selectExpr(
            f"stack({len(dq_columns)}, {stack_expr}) as (check_key, value)",
        )

    agg_df = unpivoted_df.groupBy("check_key").agg(
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
    agg_df = agg_df.withColumn("dq_column", f.col("check_key"))
    agg_df = agg_df.withColumn("column", f.split(f.col("check_key"), "-").getItem(1))
    agg_df = agg_df.withColumn("assertion", f.split(f.col("check_key"), "-").getItem(0))

    # get data descriptions from nocodb
    dq_column_name_table_id = get_nocodb_table_id_from_name(
        table_name="SchoolGeolocationMasterDQChecks"
    )
    dq_column_name_table = get_nocodb_table_as_pandas_dataframe(
        table_id=dq_column_name_table_id
    )

    dq_column_name_table["merge_col"] = dq_column_name_table[
        "DQ Table Column Name"
    ].map(lambda v: v.replace("dq_", ""))
    dq_column_name_table = dq_column_name_table.rename(
        columns={"DQ Check Category": "type", "Human Readable Name": "description"}
    )
    dq_column_name_df = spark.createDataFrame(dq_column_name_table)

    report = agg_df.join(
        dq_column_name_df.select(*["type", "description", "merge_col"]),
        agg_df["dq_column"] == dq_column_name_df["merge_col"],
        how="inner",
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

    df_aggregated = df_aggregated.withColumn(
        "is_critical_dq_check",
        f.when(f.col("type") == "critical checks", 1).otherwise(0),
    )

    critical_checks_df = df_aggregated[df_aggregated.is_critical_dq_check == 1]
    critical_checks_df = critical_checks_df.drop("is_critical_dq_check", "type")
    critical_checks_summary = critical_checks_df.toPandas().to_dict(orient="records")

    df_aggregated = df_aggregated[df_aggregated.is_critical_dq_check != 1]
    df_aggregated = df_aggregated.drop("is_critical_dq_check")

    # Initialize an empty dictionary for the transformed data
    agg_array = df_aggregated.toPandas().to_dict(orient="records")
    transformed_data = {"summary": summary, "critical_checks": critical_checks_summary}

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
    df = _normalize_dq_results_map(df)

    # dq_results is now a map<string, int>; access specific keys via element_at().
    def _check(key):
        return f.element_at(f.col("dq_results"), key)

    # Computed composite columns derived from map values
    df = df.withColumn(
        "dq_missing_location",
        f.when(
            (_check("is_null_optional-latitude") == 1)
            | (_check("is_null_optional-longitude") == 1),
            1,
        ).otherwise(0),
    )

    df = df.withColumn(
        "dq_is_null_connectivity_type_when_connectivity_govt",
        f.when(
            (_check("is_null_optional-connectivity_type_govt") == 1)
            & (_check("is_null_optional-connectivity_govt") == 0),
            1,
        ).otherwise(0),
    )

    count_schools_raw_file = df.count()

    # Keys in the dq_results map needed for the statistics report
    static_check_keys = [
        "duplicate-school_id_govt",
        "duplicate_all_except_school_code",
        "duplicate_name_level_within_110m_radius",
        "duplicate_set-location_id",
        "duplicate_set-education_level_location_id",
        "duplicate_set-school_id_govt_school_name_education_level_location_id",
        "duplicate_set-school_name_education_level_location_id",
        "duplicate_similar_name_same_level_within_110m_radius",
        "is_not_within_country",
        "is_not_alphanumeric-school_name",
        "is_null_mandatory-school_id_govt",
        "is_null_optional-computer_availability",
        "is_null_optional-connectivity_govt",
        "is_null_optional-education_level_govt",
        "is_null_optional-school_name",
        "is_school_density_greater_than_5",
        "precision-latitude",
        "precision-longitude",
    ]

    # Flatten only the needed map keys into individual columns for the stack/agg step
    df_report = df.select(
        *[_check(key).cast("int").alias(f"dq_{key}") for key in static_check_keys],
        f.col("dq_has_critical_error").cast("int"),
        f.col("dq_missing_location").cast("int"),
        f.col("dq_is_null_connectivity_type_when_connectivity_govt").cast("int"),
    )

    dq_report_columns = [f"dq_{key}" for key in static_check_keys] + [
        "dq_has_critical_error",
        "dq_missing_location",
        "dq_is_null_connectivity_type_when_connectivity_govt",
    ]

    dq_duplicate_columns = [
        col for col in dq_report_columns if col.startswith("dq_duplicate")
    ]
    check_duplicate_columns = [
        f.col(duplicate_col) == 1 for duplicate_col in dq_duplicate_columns
    ]

    df_report = df_report.withColumn(
        "dq_suspected_duplicate",
        f.when(f.greatest(*check_duplicate_columns), 1).otherwise(0),
    )
    dq_report_columns.append("dq_suspected_duplicate")

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

    columns_in_dataset = agg_df_pd.index.tolist()

    # extract required statistics
    count_unique_schools_ids = df.select("school_id_govt").distinct().count()
    stats = {
        "country": upload_details["country_code"],
        "file_name": upload_details["file_name"],
        "uploaded_columns_not_used": upload_details["uploaded_columns_not_used"],
        "important_columns_not_uploaded": upload_details[
            "important_columns_not_uploaded"
        ],
        "count_schools_raw_file": count_schools_raw_file,
        "count_schools_dropped": agg_df_pd.at["has_critical_error", "count_schools"],
        "count_schools_passed": count_schools_raw_file
        - agg_df_pd.at["has_critical_error", "count_schools"],
        "count_schools_no_location": agg_df_pd.at["missing_location", "count_schools"],
        "percent_schools_no_location": round(
            (
                100
                * agg_df_pd.at["missing_location", "count_schools"]
                / count_schools_raw_file
            ),
            2,
        ),
        "count_schools_outside_country": agg_df_pd.at[
            "is_not_within_country", "count_schools"
        ],
        "percent_schools_outside_country": round(
            (
                100
                * agg_df_pd.at["is_not_within_country", "count_schools"]
                / count_schools_raw_file
            ),
            2,
        ),
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
        "count_invalid_school_name": agg_df_pd.at[
            "is_not_alphanumeric-school_name", "count_schools"
        ],
        "percent_invalid_school_name": round(
            (
                100
                * agg_df_pd.at["is_not_alphanumeric-school_name", "count_schools"]
                / count_schools_raw_file
            ),
            2,
        ),
        "count_null_school_name": agg_df_pd.at[
            "is_null_optional-school_name", "count_schools"
        ],
        "percent_null_school_name": round(
            (
                100
                * agg_df_pd.at["is_null_optional-school_name", "count_schools"]
                / count_schools_raw_file
            ),
            2,
        ),
        "count_null_education_level_govt": agg_df_pd.at[
            "is_null_optional-education_level_govt", "count_schools"
        ],
        "percent_null_education_level_govt": round(
            (
                100
                * agg_df_pd.at["is_null_optional-education_level_govt", "count_schools"]
                / count_schools_raw_file
            ),
            2,
        ),
        "count_null_connectivity_govt": agg_df_pd.at[
            "is_null_optional-connectivity_govt", "count_schools"
        ],
        "percent_null_connectivity_govt": round(
            (
                100
                * agg_df_pd.at["is_null_optional-connectivity_govt", "count_schools"]
                / count_schools_raw_file
            ),
            2,
        ),
        "count_null_connectivity_type_govt_when_connectivity": agg_df_pd.at[
            "is_null_connectivity_type_when_connectivity_govt", "count_schools"
        ],
        "percent_null_connectivity_type_govt_when_connectivity": round(
            (
                100
                * agg_df_pd.at[
                    "is_null_connectivity_type_when_connectivity_govt", "count_schools"
                ]
                / count_schools_raw_file
            ),
            2,
        ),
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
        "count_duplicate_location_id": agg_df_pd.at[
            "duplicate_set-location_id", "count_schools"
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

    def add_null_count_and_pct(dq_column_name):
        col_base_name = dq_column_name.split("-")[-1]
        counts_name = f"count_null_{col_base_name}"
        pct_name = f"percent_null_{col_base_name}"

        column_count = agg_df_pd.at["dq_column_name", "count_schools"]
        stats[counts_name] = column_count
        stats[pct_name] = round(100 * column_count / count_schools_raw_file, 2)

    cols_null_counts_to_add = [
        "computer_availability",
        "num_computers",
        "num_students",
        "num_teachers",
    ]

    for column in cols_null_counts_to_add:
        if column in columns_in_dataset:
            add_null_count_and_pct(column)

    def add_column_counts_summary(column):
        column_counts = df.groupBy(column).count().toPandas().fillna("Unknown")
        if not (list(column_counts[column].unique()) == ["Unknown"]):
            # Ensure there is no column header
            column_counts.columns = [None] * len(column_counts.columns)
            column_counts_string = column_counts.to_string(index=False)
            column_counts_string = column_counts_string.split("\n", 1)[1]
            stats[f"{column}_counts"] = column_counts_string

    cols_for_counts_summary = [
        "education_level_govt",
        "connectivity_govt",
        "connectivity_type_govt",
        "computer_availability",
        "electricity_availability",
        "water_availability",
        "school_area_type",
        "school_funding_type",
    ]

    for column in cols_for_counts_summary:
        if column in columns_in_dataset:
            add_column_counts_summary(column)

    report_template_string = get_report_template()
    report_template = Environment(loader=BaseLoader()).from_string(
        report_template_string
    )
    data_quality_report = report_template.render(**stats)

    return data_quality_report


def get_report_template() -> str:
    container_name = settings.AZURE_BLOB_CONTAINER_NAME
    service = get_blob_service_client()
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


def _normalize_dq_results_map(df: sql.DataFrame) -> sql.DataFrame:
    """Ensure dq_results is MapType(string, int).

    ADLSPandasIOManager round-trips through Pandas/PyArrow, which writes the map as a
    Parquet STRUCT (fixed keys, more compact). Spark reads it back as StructType, which
    breaks map_filter / element_at. Converting via to_json → from_json restores the
    proper MapType regardless of the original column type.
    """
    if "dq_results" not in df.columns:
        return df
    from pyspark.sql.types import StructType as _StructType

    if isinstance(df.schema["dq_results"].dataType, _StructType):
        df = df.withColumn(
            "dq_results",
            f.from_json(
                f.to_json(f.col("dq_results")), MapType(StringType(), IntegerType())
            ),
        )
    return df


def dq_geolocation_extract_relevant_columns(
    df: sql.DataFrame, uploaded_columns: list[str], mode: str
):
    df = _normalize_dq_results_map(df)

    dq_column_name_table_id = get_nocodb_table_id_from_name(
        table_name="SchoolGeolocationMasterDQChecks"
    )
    dq_column_name_table = get_nocodb_table_as_pandas_dataframe(
        table_id=dq_column_name_table_id
    )

    mode_column = f"{mode.title()} DQ"

    dq_table_mandatory = dq_column_name_table[
        dq_column_name_table[mode_column].str.lower() == "always"
    ]
    dq_table_optional = dq_column_name_table.loc[
        dq_column_name_table[mode_column].str.lower() == "if in file"
    ]

    # get relevant dq checks from the checks that involve multiple columns
    dq_table_optional_combination = dq_table_optional[
        dq_table_optional["Column Checked"]
        .str.strip()
        .str.split(",")
        .str.len()
        .fillna(0)
        .astype(int)
        > 1
    ]

    dq_table_optional_combination = dq_table_optional_combination[
        dq_table_optional_combination["Column Checked"].map(
            lambda columns: {col.strip() for col in columns.split(",")}.issubset(
                uploaded_columns
            )
        )
    ]

    # for the other optional checks, filter out for the relevant columns
    dq_table_optional = dq_table_optional[
        dq_table_optional["Column Checked"].isin(uploaded_columns)
    ]

    dq_table_all = pd.concat(
        [dq_table_mandatory, dq_table_optional, dq_table_optional_combination]
    )

    dq_columns_list = dq_table_all.sort_values("Related Check ID")[
        "DQ Table Column Name"
    ].tolist()

    # The dq_results map keys are the check names without the "dq_" prefix.
    relevant_map_keys = [col.replace("dq_", "", 1) for col in dq_columns_list]

    columns_to_keep = [
        col
        for col in [
            *uploaded_columns,
            "dq_has_critical_error",
            "failure_reason",
            "dq_results",
        ]
        if col in df.columns
    ]
    df = df.select(*columns_to_keep)

    # Narrow the dq_results map to only checks that are relevant for the uploaded
    # columns and mode. This avoids exposing checks that do not apply.
    if relevant_map_keys:
        relevant_keys_set = set(relevant_map_keys)
        df = df.withColumn(
            "dq_results",
            f.map_filter(
                f.col("dq_results"), lambda k, _v: k.isin(list(relevant_keys_set))
            ),
        )

    # human_readable_mappings: map key (without "dq_" prefix) -> human-readable label
    human_readable_mappings = {
        col.replace("dq_", "", 1): name
        for col, name in dq_table_all.set_index("DQ Table Column Name")[
            "Human Readable Name"
        ]
        .to_dict()
        .items()
    }

    return df, human_readable_mappings


def run_master_checks(
    df: sql.DataFrame,
    dq_context: DQContext,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    df = is_not_within_country(df, dq_context.country_code_iso3, context)
    df = similar_name_level_within_110_check(df, context)
    df = school_density_check(df, context)
    df = standard_checks(df, dq_context.dataset_type, context)
    df = duplicate_all_except_checks(
        df,
        CONFIG_COLUMNS_EXCEPT_SCHOOL_ID[dq_context.dataset_type],
        context,
    )
    df = precision_check(df, config.PRECISION, context)
    df = duplicate_set_checks(df, config.UNIQUE_SET_COLUMNS, context)
    df = duplicate_name_level_110_check(df, context)
    df = column_relation_checks(df, dq_context.dataset_type, context)
    df = critical_error_checks(
        df,
        dq_context.dataset_type,
        CONFIG_NONEMPTY_COLUMNS[dq_context.dataset_type],
        context,
    )
    return df


def run_geolocation_checks(
    df: sql.DataFrame,
    dq_context: DQContext,
    silver: sql.DataFrame = None,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    if dq_context.dq_mode == DQMode.MASTER:
        if dq_context.upload_mode == UploadMode.CREATE.value:
            df = create_checks(bronze=df, silver=silver, context=context)
        elif dq_context.upload_mode == UploadMode.UPDATE.value:
            df = update_checks(bronze=df, silver=silver, context=context)
    else:
        # For assessment-only (uploaded mode), skip cross-checks against silver
        # but ensure the columns exist to avoid downstream errors.
        if dq_context.upload_mode == UploadMode.CREATE.value:
            df = df.withColumn("dq_is_not_create", f.lit(0))
        elif dq_context.upload_mode == UploadMode.UPDATE.value:
            df = df.withColumn("dq_is_not_update", f.lit(0))

    df = is_not_within_country(df, dq_context.country_code_iso3, context)
    df = similar_name_level_within_110_check(df, context)
    df = school_density_check(df, context)
    df = standard_checks(df, dq_context.dataset_type, context)
    df = duplicate_all_except_checks(
        df,
        CONFIG_COLUMNS_EXCEPT_SCHOOL_ID[dq_context.dataset_type],
        context,
    )
    df = precision_check(df, config.PRECISION, context)
    df = duplicate_set_checks(df, config.UNIQUE_SET_COLUMNS, context)
    df = duplicate_name_level_110_check(df, context)
    df = critical_error_checks(
        df,
        dq_context.dataset_type,
        CONFIG_NONEMPTY_COLUMNS[dq_context.dataset_type],
        dq_context.upload_mode,
        context,
    )
    df = column_relation_checks(df, dq_context.dataset_type, context)
    return df


def run_reference_checks(
    df: sql.DataFrame,
    dq_context: DQContext,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    df = standard_checks(df, dq_context.dataset_type, context)
    df = critical_error_checks(
        df,
        dq_context.dataset_type,
        CONFIG_NONEMPTY_COLUMNS[dq_context.dataset_type],
        context,
    )
    return df


def run_coverage_checks(
    df: sql.DataFrame,
    dq_context: DQContext,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    df = standard_checks(df, dq_context.dataset_type, context)
    df = column_relation_checks(df, dq_context.dataset_type, context)
    df = critical_error_checks(
        df,
        dq_context.dataset_type,
        CONFIG_NONEMPTY_COLUMNS[dq_context.dataset_type],
        context,
    )
    return df


def run_coverage_fb_checks(
    df: sql.DataFrame,
    dq_context: DQContext,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    df = standard_checks(
        df, dq_context.dataset_type, context, domain=False, range_=False
    )
    df = fb_percent_sum_to_100_check(df, context)
    df = column_relation_checks(df, dq_context.dataset_type, context)
    df = critical_error_checks(
        df,
        dq_context.dataset_type,
        CONFIG_NONEMPTY_COLUMNS[dq_context.dataset_type],
        context,
    )
    return df


def run_qos_checks(
    df: sql.DataFrame,
    dq_context: DQContext,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    df = standard_checks(
        df, dq_context.dataset_type, context, domain=False, range_=False
    )
    df = critical_error_checks(
        df,
        dq_context.dataset_type,
        CONFIG_NONEMPTY_COLUMNS[dq_context.dataset_type],
        context,
    )
    return df


def row_level_checks_internal(
    df: sql.DataFrame,
    dq_context: DQContext,
    silver: sql.DataFrame = None,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    logger = get_context_with_fallback_logger(context)
    logger.info(
        "Starting row level checks",
        extra={
            "dq_mode": dq_context.mode.value,
            "dataset_type": dq_context.dataset_type,
            "country": dq_context.country_code_iso3,
            "upload_mode": dq_context.upload_mode,
        },
    )

    if dq_context.dataset_type == "master":
        df = run_master_checks(df, dq_context, context)
    elif dq_context.dataset_type == "geolocation":
        df = run_geolocation_checks(df, dq_context, silver, context)
    elif dq_context.dataset_type == "reference":
        df = run_reference_checks(df, dq_context, context)
    elif dq_context.dataset_type in ["coverage", "coverage_itu"]:
        df = run_coverage_checks(df, dq_context, context)
    elif dq_context.dataset_type == "coverage_fb":
        df = run_coverage_fb_checks(df, dq_context, context)
    elif dq_context.dataset_type == "qos":
        df = run_qos_checks(df, dq_context, context)

    return df


def row_level_checks(
    df: sql.DataFrame,
    dq_context: Any = None,
    _country_code_iso3: str = None,
    silver: sql.DataFrame = None,
    mode: str = None,
    context: OpExecutionContext = None,
    dataset_type: str = None,
) -> sql.DataFrame:
    # Resolve which signature is being used
    if isinstance(dq_context, DQContext):
        # Modern signature: row_level_checks(df, dq_context=DQContext(...), ...)
        return row_level_checks_internal(df, dq_context, silver, context)
    else:
        # Legacy signature: row_level_checks(df, dataset_type, country_code, ...)
        if dq_context is not None and dataset_type is None:
            dataset_type = dq_context  # Positional dataset_type

        # Build a temporary DQContext for internal processing

        internal_context = DQContext(
            dq_mode=DQMode.MASTER,  # Legacy calls default to MASTER
            dataset_type=dataset_type,
            country_code_iso3=_country_code_iso3,
            upload_mode=mode,
        )
        return row_level_checks_internal(df, internal_context, silver, context)


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
