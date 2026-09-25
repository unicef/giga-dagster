import base64
import json
import math
from datetime import UTC, date, datetime, timedelta

import numpy as np
import pandas as pd
from dagster_pyspark import PySparkResource
from google.cloud import bigquery
from google.oauth2 import service_account
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.settings import settings
from src.utils.delta import check_table_exists, create_delta_table, create_schema

from dagster import OpExecutionContext, Output, asset

SCHEMA_NAME = "giga_meter"
TABLE_NAME = "mlab_traceroutes"
FULL_TABLE_NAME = f"{SCHEMA_NAME}.{TABLE_NAME}"

# Billing/job project for the service account — separate from mlab-collaboration,
# which only grants read access to the source dataset, not bigquery.jobs.create.
BQ_PROJECT = "measurement-lab"
SOURCE_TABLE = "mlab-collaboration.hermes_union.giga_meter_measurements"

# First run has no watermark; bootstrap from here.
BOOTSTRAP_START_DATE = date(2025, 12, 2)
# Each BigQuery query covers at most this many days, so a large backlog is
# caught up over several windows within a single run rather than trickling
# in one day per scheduled run.
WINDOW_DAYS = 20

# Kept in sync with the M-Lab archiver's column projection
# (scripts/archive_giga_traceroutes.py in m-lab/2026-04-giga-traceroute).
_SELECT_COLUMNS = """\
  id,
  partition_date,
  window_start,
  src AS client_ip,
  src_country,
  src_state,
  src_lat,
  src_lon,
  src_city,
  src_asn,
  src_asn_name,
  dst_country,
  dst_lat,
  dst_lon,
  dst_city,
  dst_site,
  dst_asn,
  ndt_rtt,
  ndt_throughput,
  ndt_loss_rate,
  forward_updated_node_details,
  reverse_updated_node_details,
  forward_distance,
  reverse_distance,
  is_reaching_dst_asn"""

TABLE_SCHEMA: list[StructField] = [
    StructField("id", StringType(), True),
    StructField("partition_date", DateType(), False),
    StructField("window_start", TimestampType(), True),
    StructField("client_ip", StringType(), True),
    StructField("src_country", StringType(), True),
    StructField("src_state", StringType(), True),
    StructField("src_lat", DoubleType(), True),
    StructField("src_lon", DoubleType(), True),
    StructField("src_city", StringType(), True),
    StructField("src_asn", LongType(), True),
    StructField("src_asn_name", StringType(), True),
    StructField("dst_country", StringType(), True),
    StructField("dst_lat", DoubleType(), True),
    StructField("dst_lon", DoubleType(), True),
    StructField("dst_city", StringType(), True),
    StructField("dst_site", StringType(), True),
    StructField("dst_asn", LongType(), True),
    StructField("ndt_rtt", DoubleType(), True),
    StructField("ndt_throughput", DoubleType(), True),
    StructField("ndt_loss_rate", DoubleType(), True),
    StructField("forward_updated_node_details", StringType(), True),
    StructField("reverse_updated_node_details", StringType(), True),
    StructField("forward_distance", DoubleType(), True),
    StructField("reverse_distance", DoubleType(), True),
    StructField("is_reaching_dst_asn", BooleanType(), True),
]


def _get_bigquery_client() -> bigquery.Client:
    key_json = base64.b64decode(settings.MLAB_BIGQUERY_SERVICE_ACCOUNT_JSON_B64)
    credentials_info = json.loads(key_json)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info
    )
    return bigquery.Client(project=BQ_PROJECT, credentials=credentials)


def _build_query() -> str:
    return (
        f"SELECT\n{_SELECT_COLUMNS}\n"  # nosec B608
        f"FROM `{SOURCE_TABLE}`\n"
        "WHERE partition_date > @lower_exclusive\n"
        "  AND partition_date <= @upper_inclusive\n"
    )


def _fetch_window(
    client: bigquery.Client, lower_exclusive: date, upper_inclusive: date, log
) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lower_exclusive", "DATE", lower_exclusive),
            bigquery.ScalarQueryParameter("upper_inclusive", "DATE", upper_inclusive),
        ]
    )
    query_job = client.query(_build_query(), job_config=job_config)
    log.info(f"Submitted BigQuery job {query_job.job_id}")
    result = query_job.result()
    log.info(
        f"BigQuery job {query_job.job_id} finished: {result.total_rows} rows, "
        f"{query_job.total_bytes_processed} bytes processed. Downloading to pandas..."
    )
    pdf = result.to_dataframe(create_bqstorage_client=False)
    log.info(
        f"Downloaded {len(pdf)} rows to pandas for window ending {upper_inclusive}"
    )
    return pdf


def _json_safe(value):
    """Recursively convert numpy/datetime values from a BigQuery RECORD field
    into plain JSON-serializable Python types (json.dumps does not recurse into
    numpy arrays on its own — it just stringifies the whole thing via `default`)."""
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple | np.ndarray):
        return [_json_safe(v) for v in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _to_json_string(value) -> str | None:
    """Flatten a possibly-nested BigQuery RECORD/REPEATED value to a JSON string."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    return json.dumps(_json_safe(value), default=str)


def _prepare_window_df(window_pdf: pd.DataFrame) -> pd.DataFrame:
    window_pdf = window_pdf[[field.name for field in TABLE_SCHEMA]].copy()
    window_pdf["forward_updated_node_details"] = window_pdf[
        "forward_updated_node_details"
    ].map(_to_json_string)
    window_pdf["reverse_updated_node_details"] = window_pdf[
        "reverse_updated_node_details"
    ].map(_to_json_string)
    # Normalize BigQuery's extension dtypes (dbdate, nullable Int64/boolean) to plain
    # Python objects so Spark's createDataFrame can apply TABLE_SCHEMA directly.
    return window_pdf.astype(object).where(window_pdf.notna(), None)


@asset
def mlab_traceroutes(context: OpExecutionContext, spark: PySparkResource) -> Output:
    s: SparkSession = spark.spark_session

    table_exists = check_table_exists(s, SCHEMA_NAME, TABLE_NAME, None)

    if table_exists:
        lower_exclusive = s.sql(
            f"SELECT MAX(partition_date) AS last_date FROM {FULL_TABLE_NAME}"  # nosec B608
        ).collect()[0]["last_date"]
        context.log.info(f"Resuming from watermark {lower_exclusive}")
    else:
        lower_exclusive = BOOTSTRAP_START_DATE - timedelta(days=1)
        context.log.info(
            f"No existing table; bootstrapping from {BOOTSTRAP_START_DATE}"
        )

    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    context.log.info("Authenticating BigQuery client...")
    client = _get_bigquery_client()
    context.log.info(f"BigQuery client ready, billing project {client.project}")

    total_rows = 0
    windows_pulled = 0
    current_lower = lower_exclusive

    while current_lower < yesterday:
        window_upper = min(current_lower + timedelta(days=WINDOW_DAYS), yesterday)
        context.log.info(f"Pulling partition_date in ({current_lower}, {window_upper}]")
        window_pdf = _fetch_window(client, current_lower, window_upper, context.log)

        if not window_pdf.empty:
            window_pdf = _prepare_window_df(window_pdf)
            context.log.info("Converting pandas DataFrame to Spark...")
            window_sdf = s.createDataFrame(window_pdf, schema=StructType(TABLE_SCHEMA))

            if not table_exists:
                context.log.info(f"Creating {FULL_TABLE_NAME}")
                create_schema(s, SCHEMA_NAME)
                create_delta_table(
                    s,
                    SCHEMA_NAME,
                    TABLE_NAME,
                    TABLE_SCHEMA,
                    context,
                    if_not_exists=True,
                    partition_by=["partition_date"],
                )
                table_exists = True

            context.log.info(f"Writing to {FULL_TABLE_NAME}...")
            window_sdf.write.format("delta").mode("append").saveAsTable(FULL_TABLE_NAME)
            total_rows += len(window_pdf)
            windows_pulled += 1
            context.log.info(
                f"Wrote {len(window_pdf)} rows for window ending {window_upper}"
            )
        else:
            context.log.info("No rows in this window")

        current_lower = window_upper

    context.add_output_metadata(
        {
            "rows_pulled": total_rows,
            "windows_pulled": windows_pulled,
            "final_watermark": str(current_lower),
        }
    )

    return Output(None)
