from pyspark.sql import SparkSession
from src.spark import spark
from src.utils.schema import construct_full_table_name

from dagster import asset


@asset(
    group_name="maintenance",
    description="Optimizes all school_geolocation_error_table.* tables using Z-Ordering on giga_sync_file_id.",
)
def optimize_geolocation_error_table(context) -> None:
    """
    Discovers all per-country upload_errors tables and runs OPTIMIZE + ZORDER BY
    on each one to improve query performance for the Error Table UI.
    """
    s: SparkSession = spark.spark_session
    schema_name = "school_geolocation_error_table"

    context.log.info("Discovering all tables...")

    # Query the metastore for all upload_errors tables
    tables_df = s.sql(f"SHOW TABLES IN {schema_name} LIKE '*'")
    table_names = [row.tableName for row in tables_df.collect()]

    if not table_names:
        context.log.warning("No tables found. Skipping optimization.")
        return

    context.log.info(f"Found {len(table_names)} tables to optimize.")

    for table_name in table_names:
        full_table_name = construct_full_table_name(schema_name, table_name)
        try:
            query = f"OPTIMIZE {full_table_name} ZORDER BY (giga_sync_file_id)"
            context.log.info(f"Executing: {query}")
            s.sql(query).collect()
            context.log.info(f"Optimization completed for {full_table_name}.")
        except Exception as exc:
            context.log.warning(f"Failed to optimize {full_table_name}: {exc}")
