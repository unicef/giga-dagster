from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.settings import settings
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, Output, asset


@asset
@capture_op_exceptions
def adhoc__migrate_hms_table_locations_to_abfss(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> Output[None]:
    """
    One-shot migration asset that updates all Hive Metastore table locations
    from wasbs:// (Azure Blob Storage) to abfss:// (Azure Data Lake Storage Gen2).

    Run this once after deploying the abfss:// storage migration to update
    existing Delta table locations registered in the metastore. Once all
    locations are migrated, the WASBS SAS credential in spark.py can be removed.
    """
    s: SparkSession = spark.spark_session

    old_prefix = (
        f"wasbs://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_BLOB_SAS_HOST}"
    )
    new_prefix = (
        f"abfss://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_DFS_SAS_HOST}"
    )

    context.log.info(f"Migrating HMS locations: {old_prefix} -> {new_prefix}")

    databases = [row.namespace for row in s.sql("SHOW DATABASES").collect()]
    context.log.info(f"Found {len(databases)} databases: {databases}")

    migrated = []
    skipped = []
    errors = []

    for db in databases:
        tables = s.sql(f"SHOW TABLES IN `{db}`").collect()
        context.log.info(f"Database `{db}`: {len(tables)} tables")

        for table_row in tables:
            table_name = table_row.tableName
            full_name = f"`{db}`.`{table_name}`"

            try:
                detail = s.sql(f"DESCRIBE DETAIL {full_name}").collect()
                location = detail[0]["location"] if detail else None
            except Exception as e:
                context.log.warning(f"Could not DESCRIBE DETAIL {full_name}: {e}")
                errors.append({"table": full_name, "error": str(e)})
                continue

            if not location:
                context.log.info(f"  {full_name}: no location, skipping")
                skipped.append(full_name)
                continue

            if not location.startswith(old_prefix):
                context.log.info(
                    f"  {full_name}: location already uses correct scheme, skipping ({location[:60]}...)"
                )
                skipped.append(full_name)
                continue

            new_location = location.replace(old_prefix, new_prefix, 1)
            context.log.info(f"  {full_name}: updating location")
            context.log.info(f"    old: {location}")
            context.log.info(f"    new: {new_location}")

            try:
                s.sql(f"ALTER TABLE {full_name} SET LOCATION '{new_location}'")
                migrated.append(
                    {"table": full_name, "old": location, "new": new_location}
                )
            except Exception as e:
                context.log.error(f"  {full_name}: ALTER TABLE failed: {e}")
                errors.append({"table": full_name, "error": str(e)})

    context.log.info(
        f"Migration complete: {len(migrated)} migrated, {len(skipped)} skipped, {len(errors)} errors"
    )

    if errors:
        context.log.error(f"Tables with errors: {[e['table'] for e in errors]}")

    return Output(
        None,
        metadata={
            "migrated_count": len(migrated),
            "skipped_count": len(skipped),
            "error_count": len(errors),
            "migrated_tables": [m["table"] for m in migrated],
            "error_tables": [e["table"] for e in errors],
        },
    )
