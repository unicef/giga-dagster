from delta import DeltaTable
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dagster import OpExecutionContext
from src.utils.delta import create_delta_table, create_schema
from src.utils.schema import construct_full_table_name


def write_fuzzy_mismatches(
    spark: SparkSession,
    mismatches: list[dict],
    file_upload_id: str,
    context: OpExecutionContext,
):
    """
    Writes fuzzy mismatch records to the school_master.fuzzy_mismatches Delta table.
    Deletes existing records for the given file_upload_id before appending new ones.
    """
    if not mismatches:
        context.log.info("No fuzzy mismatches to write.")
        return

    schema_name = "school_master"
    table_name = "fuzzy_mismatches"
    full_table_name = construct_full_table_name(schema_name, table_name)

    # Define schema for the mismatches table
    schema = StructType(
        [
            StructField("file_upload_id", StringType(), False),
            StructField("row_index", IntegerType(), True),
            StructField("school_id_govt", StringType(), True),
            StructField("column_name", StringType(), False),
            StructField("original_value", StringType(), True),
            StructField("suggested_value", StringType(), True),
            StructField("match_score", FloatType(), True),
            StructField("is_accepted", BooleanType(), True),
            StructField("final_value", StringType(), True),
            StructField("created_at", TimestampType(), True),
        ]
    )

    # Convert list of dicts to Spark DataFrame
    df = spark.createDataFrame(mismatches, schema=schema)

    # Ensure schema exists
    create_schema(spark, schema_name)

    # Ensure table exists
    create_delta_table(
        spark,
        schema_name,
        table_name,
        schema,
        context,
        if_not_exists=True,
    )

    # Delete existing entries for this file (Overwrite logic)
    try:
        if spark.catalog.tableExists(full_table_name):
            context.log.info(
                f"Deleting existing fuzzy mismatches for file_id: {file_upload_id}"
            )

            delta_table = DeltaTable.forName(spark, full_table_name)
            delta_table.delete(f.col("file_upload_id") == f.lit(file_upload_id))

    except Exception as exc:
        context.log.warning(
            f"Failed to delete existing rows (table might not exist yet): {exc}"
        )

    context.log.info(f"Appending {df.count()} fuzzy mismatches to {full_table_name}")
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .saveAsTable(full_table_name)
    )
