"""
Delta Lake optimization utilities for reducing file fragmentation and improving query performance.
"""

from pyspark.sql import SparkSession

from dagster import OpExecutionContext


def optimize_delta_table(
    spark: SparkSession,
    full_table_name: str,
    context: OpExecutionContext = None,
    z_order_columns: list[str] = None,
) -> None:
    """
    Optimize a Delta table by compacting small files into larger ones.

    This addresses the file fragmentation issue where many small Parquet files
    accumulate over time, causing memory pressure during ACID operations.

    Args:
        spark: SparkSession instance
        full_table_name: Fully qualified table name (schema.table)
        context: Dagster context for logging
        z_order_columns: Optional list of columns to Z-ORDER by for better data clustering
    """
    if context:
        context.log.info(f"Optimizing Delta table: {full_table_name}")

    # Compact small files into larger ones
    optimize_sql = f"OPTIMIZE {full_table_name}"

    # Add Z-ORDERING if columns specified (improves query performance)
    if z_order_columns:
        z_order_cols = ", ".join(z_order_columns)
        optimize_sql += f" ZORDER BY ({z_order_cols})"
        if context:
            context.log.info(f"Z-ordering by: {z_order_cols}")

    spark.sql(optimize_sql)

    if context:
        context.log.info(f"Optimization complete for {full_table_name}")


def vacuum_delta_table(
    spark: SparkSession,
    full_table_name: str,
    retention_hours: int = 168,  # 7 days default
    context: OpExecutionContext = None,
) -> None:
    """
    Remove old files from Delta table that are no longer referenced.

    This frees up storage space by removing files from previous versions
    that are older than the retention period.

    Args:
        spark: SparkSession instance
        full_table_name: Fully qualified table name (schema.table)
        retention_hours: Hours to retain old files (default 168 = 7 days)
        context: Dagster context for logging
    """
    if context:
        context.log.info(
            f"Vacuuming Delta table: {full_table_name} "
            f"(retention: {retention_hours} hours)"
        )

    vacuum_sql = f"VACUUM {full_table_name} RETAIN {retention_hours} HOURS"
    spark.sql(vacuum_sql)

    if context:
        context.log.info(f"Vacuum complete for {full_table_name}")


def get_delta_table_stats(
    spark: SparkSession,
    full_table_name: str,
    context: OpExecutionContext = None,
) -> dict:
    """
    Get statistics about a Delta table including file count and size.

    Useful for diagnosing file fragmentation issues.

    Args:
        spark: SparkSession instance
        full_table_name: Fully qualified table name (schema.table)
        context: Dagster context for logging

    Returns:
        Dictionary with table statistics
    """
    detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
    detail = detail_df.collect()[0]

    stats = {
        "num_files": detail.numFiles,
        "size_in_bytes": detail.sizeInBytes,
        "format": detail.format,
        "location": detail.location,
    }

    if context:
        context.log.info(f"Table stats for {full_table_name}:")
        context.log.info(f"  Files: {stats['num_files']}")
        context.log.info(f"  Size: {stats['size_in_bytes'] / (1024**2):.2f} MB")

    return stats


def should_optimize_table(
    spark: SparkSession,
    full_table_name: str,
    max_files_threshold: int = 100,
    context: OpExecutionContext = None,
) -> bool:
    """
    Check if a table should be optimized based on file count.

    Args:
        spark: SparkSession instance
        full_table_name: Fully qualified table name (schema.table)
        max_files_threshold: Trigger optimization if file count exceeds this
        context: Dagster context for logging

    Returns:
        True if optimization is recommended
    """
    stats = get_delta_table_stats(spark, full_table_name, context)
    num_files = stats["num_files"]

    should_optimize = num_files > max_files_threshold

    if context and should_optimize:
        context.log.warning(
            f"Table {full_table_name} has {num_files} files "
            f"(threshold: {max_files_threshold}). Optimization recommended."
        )

    return should_optimize
