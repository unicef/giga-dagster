from pyspark.sql import SparkSession
from src.spark import spark
from src.utils.schema import construct_full_table_name

from dagster import asset


@asset(
    group_name="maintenance",
    description="Optimizes all school_master.upload_errors_* tables using Z-Ordering on dataset_type.",
)
def optimize_upload_errors_table(context) -> None:
    """
    Discovers all per-country upload_errors tables and runs OPTIMIZE + ZORDER BY
    on each one to improve query performance for the Error Table UI.
    """
    s: SparkSession = spark.spark_session
    schema_name = "school_master"

    context.log.info("Discovering upload_errors_* tables...")

    # Query the metastore for all upload_errors tables
    tables_df = s.sql(f"SHOW TABLES IN {schema_name} LIKE 'upload_errors_*'")
    table_names = [row.tableName for row in tables_df.collect()]

    if not table_names:
        context.log.warning("No upload_errors_* tables found. Skipping optimization.")
        return

    context.log.info(f"Found {len(table_names)} upload_errors tables to optimize.")

    for table_name in table_names:
        full_table_name = construct_full_table_name(schema_name, table_name)
        try:
            query = f"OPTIMIZE {full_table_name} ZORDER BY (dataset_type)"
            context.log.info(f"Executing: {query}")
            s.sql(query).collect()
            context.log.info(f"Optimization completed for {full_table_name}.")
        except Exception as exc:
            context.log.warning(f"Failed to optimize {full_table_name}: {exc}")
