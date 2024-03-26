import os

from delta import DeltaTable
from models import VALID_PRIMITIVES, Schema
from pyspark import sql
from pyspark.sql.functions import col, when
from src.utils.delta import execute_query_with_error_handler

from dagster import OpExecutionContext


def get_filepath(context: OpExecutionContext) -> str:
    return context.run_tags["dagster/run_key"].split(":")[0]


def validate_raw_schema(context: OpExecutionContext, df: sql.DataFrame):
    filepath = get_filepath(context)

    df = df.withColumn(
        "dq_invalid_data_type",
        when(col("data_type").isin(VALID_PRIMITIVES), 0).otherwise(1),
    )
    invalid_rows_df = df.filter(col("dq_invalid_data_type") == 1)
    if invalid_rows_df.count() > 0:
        invalid_values = [
            row["data_type"] for row in invalid_rows_df.select("data_type").collect()
        ]
        message = f"Invalid data type found in `{filepath}`. Valid values are {VALID_PRIMITIVES}, found {invalid_values}."
        context.log.error(message)
        raise ValueError(message)

    return df.drop("dq_invalid_data_type")


def save_schema_delta_table(context: OpExecutionContext, df: sql.DataFrame):
    filepath = get_filepath(context)
    spark = df.sparkSession

    filename = os.path.splitext(filepath.split("/")[-1])[0]
    schema_name = Schema.__schema_name__
    table_name = filename.replace("-", "_").lower()
    full_table_name = f"{schema_name}.{table_name}"

    columns = Schema.fields
    query = (
        DeltaTable.createOrReplace(spark).tableName(full_table_name).addColumns(columns)
    )
    execute_query_with_error_handler(context, spark, query, schema_name, table_name)

    (
        DeltaTable.forName(spark, full_table_name)
        .alias("master")
        .merge(df.alias("updates"), "master.name = updates.name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
