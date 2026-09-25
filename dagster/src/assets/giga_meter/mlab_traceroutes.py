from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as f,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
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

# Target column name -> source column name, only where they differ.
_SOURCE_COLUMN_ALIASES = {"client_ip": "src"}
# BigQuery RECORD REPEATED columns, flattened to JSON strings for storage.
_JSON_ENCODE_COLUMNS = {"forward_updated_node_details", "reverse_updated_node_details"}

# Kept in sync with the M-Lab archiver's column projection
# (scripts/archive_giga_traceroutes.py in m-lab/2026-04-giga-traceroute).
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


def _build_source_df(spark: SparkSession, day: str) -> DataFrame:
    table_ref = SOURCE_TABLE.replace(".", ":", 1)  # project:dataset.table
    raw_df = (
        spark.read.format("bigquery")
        .option("table", table_ref)
        .option("parentProject", BQ_PROJECT)
        .option("credentials", settings.MLAB_BIGQUERY_SERVICE_ACCOUNT_JSON_B64)
        .load()
        .where(f.col("partition_date") == day)
    )

    select_exprs = []
    for field in TABLE_SCHEMA:
        source_name = _SOURCE_COLUMN_ALIASES.get(field.name, field.name)
        column = (
            f.to_json(source_name)
            if field.name in _JSON_ENCODE_COLUMNS
            else f.col(source_name)
        )
        select_exprs.append(column.cast(field.dataType).alias(field.name))

    return raw_df.select(*select_exprs)


@asset(partitions_def=mlab_traceroutes_partitions_def)
def mlab_traceroutes(context: OpExecutionContext, spark: PySparkResource) -> Output:
    s: SparkSession = spark.spark_session
    day = context.partition_key

    context.log.info(f"Pulling partition_date = {day} via Spark BigQuery connector")
    source_df = _build_source_df(s, day)

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
        f"Writing to {FULL_TABLE_NAME} (replaceWhere partition_date = '{day}')"
    )
    (
        source_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"partition_date = '{day}'")
        .saveAsTable(FULL_TABLE_NAME)
    )

    metrics = (
        DeltaTable.forName(s, FULL_TABLE_NAME)
        .history(1)
        .select("operationMetrics")
        .collect()[0]["operationMetrics"]
    )
    rows_written = metrics.get("numOutputRows")
    context.log.info(f"Wrote {rows_written} rows for partition {day}")

    context.add_output_metadata({"partition": day, "rows_written": rows_written})

    return Output(None)
