import base64
import json
import math
from datetime import date, datetime

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
from src.partitions.mlab_traceroutes import mlab_traceroutes_partitions_def
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
        "WHERE partition_date = @day\n"
    )


def _fetch_day(client: bigquery.Client, day: date, log) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    )
    query_job = client.query(_build_query(), job_config=job_config)
    log.info(f"Submitted BigQuery job {query_job.job_id}")
    result = query_job.result()
    log.info(
        f"BigQuery job {query_job.job_id} finished: {result.total_rows} rows. "
        "Downloading to pandas..."
    )
    pdf = result.to_dataframe(create_bqstorage_client=False)
    log.info(f"Downloaded {len(pdf)} rows to pandas for {day}")
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


def _prepare_day_df(day_pdf: pd.DataFrame) -> pd.DataFrame:
    day_pdf = day_pdf[[field.name for field in TABLE_SCHEMA]].copy()
    day_pdf["forward_updated_node_details"] = day_pdf[
        "forward_updated_node_details"
    ].map(_to_json_string)
    day_pdf["reverse_updated_node_details"] = day_pdf[
        "reverse_updated_node_details"
    ].map(_to_json_string)
    # Normalize BigQuery's extension dtypes (dbdate, nullable Int64/boolean) to plain
    # Python objects so Spark's createDataFrame can apply TABLE_SCHEMA directly.
    return day_pdf.astype(object).where(day_pdf.notna(), None)


@asset(partitions_def=mlab_traceroutes_partitions_def)
def mlab_traceroutes(context: OpExecutionContext, spark: PySparkResource) -> Output:
    s: SparkSession = spark.spark_session
    day_str = context.partition_key
    day = date.fromisoformat(day_str)

    context.log.info("Authenticating BigQuery client...")
    client = _get_bigquery_client()
    context.log.info(f"BigQuery client ready, billing project {client.project}")

    day_pdf = _fetch_day(client, day, context.log)

    rows_written = 0
    if not day_pdf.empty:
        day_pdf = _prepare_day_df(day_pdf)
        context.log.info("Converting pandas DataFrame to Spark...")
        day_sdf = s.createDataFrame(day_pdf, schema=StructType(TABLE_SCHEMA))

        if not check_table_exists(s, SCHEMA_NAME, TABLE_NAME, None):
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

        context.log.info(
            f"Writing to {FULL_TABLE_NAME} (replaceWhere partition_date = '{day_str}')"
        )
        (
            day_sdf.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"partition_date = '{day_str}'")
            .saveAsTable(FULL_TABLE_NAME)
        )
        rows_written = len(day_pdf)
    else:
        context.log.info(f"No rows for partition {day_str}; nothing to write")

    context.add_output_metadata({"partition": day_str, "rows_written": rows_written})

    return Output(None)
