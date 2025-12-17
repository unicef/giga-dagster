import logging

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession

from dagster import MetadataValue, OpExecutionContext, Output, asset

logger = logging.getLogger(__name__)


@asset(
    group_name="adhoc",
    description="One-time migration: Update Hive Metastore table locations from WASBS to ABFSS",
    compute_kind="spark",
)
def migrate_wasbs_to_abfss_tables(
    context: OpExecutionContext, pyspark: PySparkResource
) -> Output[dict]:
    """
    Migrates all Hive Metastore table locations from WASBS to ABFSS protocol.

    This is a one-time migration asset that should be run once per environment.
    """
    spark: SparkSession = pyspark.spark_session

    # Import migration functions (assuming they're in a utils module)
    from scripts.migrate_wasbs_to_abfss import (
        get_all_tables_with_wasbs,
        migrate_table_location,
    )

    context.log.info("Starting WASBS to ABFSS migration")

    # Get all tables with WASBS
    wasbs_tables = get_all_tables_with_wasbs(spark)
    context.log.info(f"Found {len(wasbs_tables)} tables with WASBS locations")

    # Migrate each table
    success_count = 0
    failure_count = 0

    for table_info in wasbs_tables:
        if migrate_table_location(spark, table_info, dry_run=False):
            success_count += 1
        else:
            failure_count += 1

    context.log.info(
        f"Migration complete: {success_count} successful, {failure_count} failed"
    )

    return Output(
        value={
            "total_tables": len(wasbs_tables),
            "successful": success_count,
            "failed": failure_count,
        },
        metadata={
            "total_tables": MetadataValue.int(len(wasbs_tables)),
            "successful_migrations": MetadataValue.int(success_count),
            "failed_migrations": MetadataValue.int(failure_count),
            "success_rate": MetadataValue.float(
                success_count / len(wasbs_tables) if len(wasbs_tables) > 0 else 0
            ),
        },
    )
