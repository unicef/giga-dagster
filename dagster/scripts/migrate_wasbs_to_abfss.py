#!/usr/bin/env python3
"""
WASBS to ABFSS Migration Script

This script migrates Hive Metastore table locations from the legacy WASBS protocol
to the modern ABFSS protocol for Azure Data Lake Storage Gen2.

Usage:
    # Dry run (default)
    python migrate_wasbs_to_abfss.py --dry-run

    # Execute migration
    python migrate_wasbs_to_abfss.py --execute
"""

import logging

from pyspark.sql import SparkSession
from src.settings import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_all_tables_with_wasbs(spark: SparkSession) -> list[dict[str, str]]:
    """
    Query metastore to find all tables with WASBS locations.

    Args:
        spark: Active SparkSession with Hive support enabled

    Returns:
        List of dictionaries containing table information
    """
    # Get all databases
    databases = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]
    logger.info(f"Found {len(databases)} databases to scan")

    wasbs_tables = []
    for db in databases:
        try:
            tables = spark.sql(f"SHOW TABLES IN {db}").collect()
            logger.info(f"Scanning database '{db}' ({len(tables)} tables)")

            for table_row in tables:
                table_name = table_row.tableName
                full_table_name = f"{db}.{table_name}"

                # Get table location
                desc_result = spark.sql(
                    f"DESCRIBE EXTENDED {full_table_name}"
                ).collect()
                location = None
                for row in desc_result:
                    if row.col_name == "Location":
                        location = row.data_type
                        break

                if location and location.startswith("wasbs://"):
                    wasbs_tables.append(
                        {
                            "database": db,
                            "table": table_name,
                            "full_name": full_table_name,
                            "old_location": location,
                        }
                    )
                    logger.info(f"  ✓ Found WASBS table: {full_table_name}")
                    logger.debug(f"    Location: {location}")

        except Exception as e:
            logger.warning(f"  ✗ Error processing database {db}: {e}")
            continue

    return wasbs_tables


def convert_wasbs_to_abfss(wasbs_uri: str, storage_account: str) -> str:
    """
    Convert WASBS URI to ABFSS URI.

    Args:
        wasbs_uri: Original WASBS URI
        storage_account: Azure storage account name

    Returns:
        Converted ABFSS URI

    Example:
        wasbs://container@storage.blob.core.windows.net/path
        -> abfss://container@storage.dfs.core.windows.net/path
    """
    abfss_uri = wasbs_uri.replace("wasbs://", "abfss://")
    abfss_uri = abfss_uri.replace(".blob.core.windows.net", ".dfs.core.windows.net")
    return abfss_uri


def migrate_table_location(
    spark: SparkSession, table_info: dict[str, str], dry_run: bool = True
) -> bool:
    """
    Migrate a single table from WASBS to ABFSS.

    Args:
        spark: Active SparkSession
        table_info: Dictionary containing table metadata
        dry_run: If True, only log the changes without executing

    Returns:
        True if successful, False otherwise
    """
    full_name = table_info["full_name"]
    old_location = table_info["old_location"]
    new_location = convert_wasbs_to_abfss(
        old_location, settings.AZURE_STORAGE_ACCOUNT_NAME
    )

    logger.info("=" * 80)
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Migrating {full_name}")
    logger.info(f"  Old: {old_location}")
    logger.info(f"  New: {new_location}")

    if not dry_run:
        try:
            # Execute ALTER TABLE
            alter_sql = f"ALTER TABLE {full_name} SET LOCATION '{new_location}'"
            logger.info(f"  Executing: {alter_sql}")
            spark.sql(alter_sql)

            # Verify the change
            desc_result = spark.sql(f"DESCRIBE EXTENDED {full_name}").collect()
            for row in desc_result:
                if row.col_name == "Location":
                    actual_location = row.data_type
                    if actual_location == new_location:
                        logger.info(f"Successfully migrated {full_name}")
                        logger.info(f"Verified new location: {actual_location}")
                        return True
                    else:
                        logger.error("Location mismatch after migration!")
                        logger.error(f"    Expected: {new_location}")
                        logger.error(f"    Actual: {actual_location}")
                        return False

            logger.error("Could not verify location after migration")
            return False

        except Exception as e:
            logger.error(f"Failed to migrate {full_name}: {e}")
            return False
    else:
        logger.info(f"SQL: ALTER TABLE {full_name} SET LOCATION '{new_location}'")
        return True


def migrate_all_tables(dry_run: bool = True) -> dict[str, int]:
    """
    Main migration function that processes all tables.

    Args:
        dry_run: If True, only simulate the migration

    Returns:
        Dictionary with migration statistics
    """
    # Create Spark session with Hive support
    spark = (
        SparkSession.builder.appName("WASBS to ABFSS Migration")
        .enableHiveSupport()
        .getOrCreate()
    )

    logger.info("=" * 80)
    logger.info("Starting WASBS to ABFSS migration")
    logger.info(
        f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE EXECUTION'}"
    )
    logger.info(f"Storage Account: {settings.AZURE_STORAGE_ACCOUNT_NAME}")
    logger.info("=" * 80)

    # Get all tables with WASBS
    logger.info("Phase 1: Discovering tables with WASBS locations...")
    wasbs_tables = get_all_tables_with_wasbs(spark)

    logger.info("=" * 80)
    logger.info(
        f"Discovery complete: Found {len(wasbs_tables)} tables with WASBS locations"
    )
    logger.info("=" * 80)

    if len(wasbs_tables) == 0:
        logger.info("No tables to migrate. Exiting.")
        spark.stop()
        return {"total": 0, "successful": 0, "failed": 0}

    # Migrate each table
    logger.info(f"Phase 2: {'Simulating' if dry_run else 'Executing'} migration...")
    success_count = 0
    failure_count = 0

    for i, table_info in enumerate(wasbs_tables, 1):
        logger.info(f"\nProcessing table {i}/{len(wasbs_tables)}")
        if migrate_table_location(spark, table_info, dry_run):
            success_count += 1
        else:
            failure_count += 1

    # Summary
    logger.info("=" * 80)
    logger.info(f"Migration {'dry run' if dry_run else 'execution'} complete")
    logger.info(f"  Total tables: {len(wasbs_tables)}")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {failure_count}")

    if dry_run:
        logger.info("\nThis was a DRY RUN - no changes were made")
        logger.info("To execute the migration, run with --execute flag")
    else:
        logger.info("\n✓ Migration complete!")
        if failure_count > 0:
            logger.warning(
                f"{failure_count} tables failed to migrate - review logs above"
            )

    logger.info("=" * 80)

    spark.stop()

    return {
        "total": len(wasbs_tables),
        "successful": success_count,
        "failed": failure_count,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Migrate Hive Metastore table locations from WASBS to ABFSS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run (default - no changes made)
  python migrate_wasbs_to_abfss.py --dry-run

  # Execute the migration
  python migrate_wasbs_to_abfss.py --execute
        """,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Perform a dry run without making changes (default)",
    )

    parser.add_argument(
        "--execute", action="store_true", help="Execute the migration (not a dry run)"
    )

    args = parser.parse_args()

    # If --execute is specified, override dry_run
    dry_run = not args.execute

    if not dry_run:
        logger.warning("=" * 80)
        logger.warning("LIVE EXECUTION MODE - Changes will be made to the metastore!")
        logger.warning("=" * 80)
        response = input("Are you sure you want to proceed? (yes/no): ")
        if response.lower() != "yes":
            logger.info("Migration cancelled by user")
            exit(0)

    results = migrate_all_tables(dry_run=dry_run)

    # Exit with error code if any failures occurred
    if results["failed"] > 0:
        exit(1)
