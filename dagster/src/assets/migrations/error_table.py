from dagster_pyspark import PySparkResource
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, TimestampType
from src.utils.delta import create_schema
from src.utils.sentry import capture_op_exceptions

from dagster import AssetExecutionContext, asset


@asset
@capture_op_exceptions
def initialize_upload_errors_table(
    context: AssetExecutionContext, spark: PySparkResource
) -> None:
    """
    Initialize the unified upload errors table if it does not exist.
    Schema:
    - giga_sync_file_id (string)
    - giga_sync_file_name (string)
    - dataset_type (string)
    - country_code (string)
    - row_data (string)
    - error_details (string)
    - created_at (timestamp)
    """
    s: SparkSession = spark.spark_session
    schema_name = "school_master"
    table_name = "upload_errors"
    full_table_name = f"{schema_name}.{table_name}"

    context.log.info(f"Ensuring table {full_table_name} exists...")

    # Define strict schema
    columns = [
        StructField("giga_sync_file_id", StringType(), True),
        StructField("giga_sync_file_name", StringType(), True),
        StructField("dataset_type", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("row_data", StringType(), True),
        StructField("error_details", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]

    # Ensure schema exists first
    create_schema(s, schema_name)

    (
        DeltaTable.createIfNotExists(s)
        .tableName(full_table_name)
        .addColumns(columns)
        .partitionedBy("giga_sync_file_id")
        .execute()
    )

    context.log.info(f"Table {full_table_name} initialized successfully.")
