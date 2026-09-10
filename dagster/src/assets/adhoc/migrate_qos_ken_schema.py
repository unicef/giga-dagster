from dagster_pyspark import PySparkResource
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import (
    DataType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, Output, asset

TABLE_NAME = "qos.ken"

TARGET_SCHEMA: list[tuple[str, DataType]] = [
    ("timestamp", TimestampType()),
    ("country_id", StringType()),
    ("school_id_govt", StringType()),
    ("school_id_giga", StringType()),
    ("speed_download_probe", FloatType()),
    ("speed_upload_probe", FloatType()),
    ("latency_probe", FloatType()),
    ("ce_ingress", FloatType()),
    ("ce_egress", FloatType()),
    ("pe_ingress", FloatType()),
    ("pe_egress", FloatType()),
    ("provider", StringType()),
    ("speed_download", FloatType()),
    ("speed_upload", FloatType()),
    ("latency", FloatType()),
    ("signature", StringType()),
    ("gigasync_id", StringType()),
    ("date", DateType()),
    ("host_id", StringType()),
    ("speed_download_mean", FloatType()),
    ("speed_download_max", FloatType()),
    ("speed_upload_mean", FloatType()),
    ("speed_upload_max", FloatType()),
    ("latency_min", FloatType()),
    ("latency_mean", FloatType()),
    ("latency_max", FloatType()),
    ("signal_mean", FloatType()),
    ("signal_max", FloatType()),
    ("measurement_id", StringType()),
    ("is_connected_all", IntegerType()),
    ("is_connected_true", IntegerType()),
    ("speed_download_min", FloatType()),
    ("speed_upload_min", FloatType()),
    ("roundtrip_time_min", FloatType()),
    ("roundtrip_time_mean", FloatType()),
    ("roundtrip_time_max", FloatType()),
    ("count", IntegerType()),
    ("measurement_type", StringType()),
    ("roundtrip_time", FloatType()),
    ("ping_status_mean", FloatType()),
]


def _null_counts(df, column_names: list[str]) -> dict[str, int]:
    row = df.select(
        [
            f.coalesce(f.sum(f.col(name).isNull().cast("int")), f.lit(0)).alias(name)
            for name in column_names
        ]
    ).first()
    return row.asDict()


@asset
@capture_op_exceptions
def adhoc__migrate_qos_ken_schema(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> Output[None]:
    """One-shot: casts qos.ken columns to correct types, adds prd's missing columns. Materialize once per env; safe to delete after."""
    s: SparkSession = spark.spark_session

    df = s.table(TABLE_NAME)
    existing_columns = set(df.columns)
    existing_target_columns = [
        name for name, _ in TARGET_SCHEMA if name in existing_columns
    ]

    pre_null_counts = _null_counts(df, existing_target_columns)

    select_exprs = [
        f.col(name).cast(target_type).alias(name)
        if name in existing_columns
        else f.lit(None).cast(target_type).alias(name)
        for name, target_type in TARGET_SCHEMA
    ]
    new_df = df.select(*select_exprs)

    post_null_counts = _null_counts(new_df, existing_target_columns)
    new_nulls_by_column = {
        name: post_null_counts[name] - pre_null_counts[name]
        for name in existing_target_columns
        if post_null_counts[name] > pre_null_counts[name]
    }
    if new_nulls_by_column:
        context.log.warning(
            f"Casting introduced new nulls (check for unparseable values): {new_nulls_by_column}"
        )

    (
        new_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("date")
        .saveAsTable(TABLE_NAME)
    )

    return Output(
        None,
        metadata={
            "added_columns": sorted(
                name for name, _ in TARGET_SCHEMA if name not in existing_columns
            ),
            "new_nulls_by_column": new_nulls_by_column,
        },
    )
