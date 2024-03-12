from delta import DeltaTable
from models import Schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField

from dagster import (
    AssetExecutionContext,
    InputContext,
    OpExecutionContext,
    OutputContext,
)
from src.constants import constants


def get_schema_name(
    context: InputContext | OutputContext | OpExecutionContext | AssetExecutionContext,
):
    if isinstance(context, InputContext | OutputContext):
        return context.step_context.op_config["metastore_schema"]
    return context.op_config["metastore_schema"]


def get_schema_table(spark: SparkSession, schema_name: str):
    metaschema_name = Schema.__schema_name__
    full_table_name = f"{metaschema_name}.{schema_name}"

    # This should be cheap if the migrations.migrate_schema asset is caching the table properly
    return DeltaTable.forName(spark, full_table_name).toDF()


def get_schema_columns(spark: SparkSession, schema_name: str):
    df = get_schema_table(spark, schema_name)
    return [
        StructField(
            row.name,
            getattr(constants.TYPE_MAPPINGS, row.data_type).pyspark(),
            row.is_nullable,
        )
        for row in df.collect()
    ]


def get_primary_key(spark: SparkSession, schema_name: str) -> str:
    df = get_schema_table(spark, schema_name)
    return df.filter(df["primary_key"]).first().name


def get_partition_columns(spark: SparkSession, schema_name: str) -> list[str]:
    df = get_schema_table(spark, schema_name)
    return [
        row.name
        for row in df.filter(
            col("partition_order").isNotNull() & (col("partition_order") > 0)
        )
        .orderBy("partition_order")
        .select("name")
        .collect()
    ]
