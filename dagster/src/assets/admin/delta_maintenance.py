"""
Delta Lake maintenance assets for optimizing and vacuuming tables.

These assets help prevent file fragmentation issues by periodically
compacting small files and cleaning up old data.
"""

from src.resources import ResourceKey
from src.utils.delta_optimize import (
    get_delta_table_stats,
    optimize_delta_table,
    vacuum_delta_table,
)
from src.utils.spark import get_spark_session

from dagster import AssetExecutionContext, asset


@asset(
    name="optimize_all_delta_tables",
    description="Optimize all Delta tables to compact small files and improve performance",
    required_resource_keys={ResourceKey.SPARK.value},
    group_name="maintenance",
)
def optimize_all_delta_tables(context: AssetExecutionContext):
    """
    Periodically optimize all Delta tables to prevent file fragmentation.

    This should be run weekly or when tables show performance degradation.
    """
    spark = get_spark_session()

    # List of schemas and tables to optimize
    schemas_to_optimize = [
        "pipeline_tables",
        "pipeline_tables_staging",
        "qos",
        "qos_raw",
        "qos_availability",
    ]

    optimized_count = 0

    for schema in schemas_to_optimize:
        try:
            # Get all tables in schema
            tables = spark.sql(f"SHOW TABLES IN {schema}").collect()

            for table_row in tables:
                table_name = table_row.tableName
                full_table_name = f"{schema}.{table_name}"

                try:
                    # Get stats before optimization
                    stats_before = get_delta_table_stats(
                        spark, full_table_name, context
                    )

                    if stats_before["num_files"] > 10:  # Only optimize if > 10 files
                        context.log.info(
                            f"Optimizing {full_table_name} "
                            f"({stats_before['num_files']} files, "
                            f"{stats_before['size_in_bytes'] / (1024**2):.2f} MB)"
                        )

                        # Optimize with Z-ordering on common columns if they exist
                        z_order_cols = []
                        if "school_id_giga" in [
                            f.name for f in spark.table(full_table_name).schema.fields
                        ]:
                            z_order_cols.append("school_id_giga")
                        if "date" in [
                            f.name for f in spark.table(full_table_name).schema.fields
                        ]:
                            z_order_cols.append("date")

                        optimize_delta_table(
                            spark,
                            full_table_name,
                            context=context,
                            z_order_columns=z_order_cols if z_order_cols else None,
                        )

                        # Get stats after optimization
                        stats_after = get_delta_table_stats(
                            spark, full_table_name, context
                        )

                        context.log.info(
                            f"Optimization complete: {stats_before['num_files']} → "
                            f"{stats_after['num_files']} files"
                        )

                        optimized_count += 1
                    else:
                        context.log.info(
                            f"Skipping {full_table_name} "
                            f"(only {stats_before['num_files']} files)"
                        )

                except Exception as e:
                    context.log.warning(f"Failed to optimize {full_table_name}: {e}")

        except Exception as e:
            context.log.warning(f"Failed to process schema {schema}: {e}")

    context.log.info(f"Optimized {optimized_count} tables")

    return {"optimized_tables": optimized_count}


@asset(
    name="vacuum_old_delta_files",
    description="Remove old Delta table files to free up storage space",
    required_resource_keys={ResourceKey.SPARK.value},
    group_name="maintenance",
)
def vacuum_old_delta_files(context: AssetExecutionContext):
    """
    Remove old files from Delta tables that are no longer needed.

    This should be run weekly to clean up storage.
    """
    spark = get_spark_session()

    schemas_to_vacuum = [
        "pipeline_tables",
        "pipeline_tables_staging",
        "qos",
        "qos_raw",
        "qos_availability",
    ]

    vacuumed_count = 0

    for schema in schemas_to_vacuum:
        try:
            tables = spark.sql(f"SHOW TABLES IN {schema}").collect()

            for table_row in tables:
                table_name = table_row.tableName
                full_table_name = f"{schema}.{table_name}"

                try:
                    # Vacuum with 7-day retention (168 hours)
                    vacuum_delta_table(
                        spark,
                        full_table_name,
                        retention_hours=168,
                        context=context,
                    )
                    vacuumed_count += 1

                except Exception as e:
                    context.log.warning(f"Failed to vacuum {full_table_name}: {e}")

        except Exception as e:
            context.log.warning(f"Failed to process schema {schema}: {e}")

    context.log.info(f"Vacuumed {vacuumed_count} tables")

    return {"vacuumed_tables": vacuumed_count}
