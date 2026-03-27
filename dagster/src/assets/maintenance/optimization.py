from pyspark.sql import SparkSession
from src.spark import spark

from dagster import asset


@asset(
    group_name="maintenance",
    description="Optimizes the school_master.upload_errors table using Z-Ordering on country_code and dataset_type.",
)
def optimize_upload_errors_table(context) -> None:
    """
    Runs OPTIMIZE and ZORDER BY on the upload_errors table.
    This improves query performance for the Admin Dashboard which filters by country and dataset.
    """
    s: SparkSession = spark.spark_session
    full_table_name = "school_master.upload_errors"

    context.log.info(f"Starting optimization for {full_table_name}...")

    # Check if table exists first to avoid error if run before migration
    if s.catalog.tableExists(full_table_name):
        # Optimize with Z-Ordering
        # Note: Delta Lake on Spark OS supports OPTIMIZE.
        query = f"OPTIMIZE {full_table_name} ZORDER BY (country_code, dataset_type)"
        context.log.info(f"Executing: {query}")
        s.sql(query).collect()  # Trigger execution
        context.log.info("Optimization completed successfully.")
    else:
        context.log.warning(
            f"Table {full_table_name} does not exist. Skipping optimization."
        )
