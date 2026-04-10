from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.settings import settings
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, Output, asset


def _migrate_locations(
    context: OpExecutionContext,
    s: SparkSession,
    old_prefix: str,
    new_prefix: str,
) -> dict:
    """
    Migrates all HMS location URIs from old_prefix to new_prefix across all
    four tables that store paths: CTLGS, DBS, SDS, and SERDE_PARAMS.

    Returns counts of migrated/skipped/errored items per category.
    """
    results = {
        "databases": {"migrated": [], "skipped": [], "errors": []},
        "tables": {"migrated": [], "skipped": [], "errors": []},
    }

    databases = [row.namespace for row in s.sql("SHOW DATABASES").collect()]
    context.log.info(f"Found {len(databases)} databases: {databases}")

    # --- Databases (DBS.DB_LOCATION_URI) ---
    # CTLGS.LOCATION_URI has no Spark SQL equivalent — must be updated via
    # direct SQL on the HMS PostgreSQL database. See docs/abfss-migration.md.
    for db in databases:
        try:
            db_detail = s.sql(f"DESCRIBE DATABASE EXTENDED `{db}`").collect()
            db_location = next(
                (
                    r["database_description_value"]
                    for r in db_detail
                    if r["database_description_item"] == "Location"
                ),
                None,
            )
        except Exception as e:
            context.log.warning(f"Could not DESCRIBE DATABASE `{db}`: {e}")
            results["databases"]["errors"].append({"db": db, "error": str(e)})
            continue

        if not db_location or not db_location.startswith(old_prefix):
            context.log.info(
                f"  DB `{db}`: location does not match, skipping ({db_location})"
            )
            results["databases"]["skipped"].append(db)
        else:
            new_db_location = db_location.replace(old_prefix, new_prefix, 1)
            context.log.info(f"  DB `{db}`: updating location")
            context.log.info(f"    old: {db_location}")
            context.log.info(f"    new: {new_db_location}")
            try:
                s.sql(f"ALTER DATABASE `{db}` SET LOCATION '{new_db_location}'")
                results["databases"]["migrated"].append(
                    {"db": db, "old": db_location, "new": new_db_location}
                )
            except Exception as e:
                context.log.error(f"  DB `{db}`: ALTER DATABASE failed: {e}")
                results["databases"]["errors"].append({"db": db, "error": str(e)})

        # --- Tables (SDS.LOCATION + SERDE_PARAMS.path) ---
        tables = s.sql(f"SHOW TABLES IN `{db}`").collect()
        context.log.info(f"Database `{db}`: {len(tables)} tables")

        for table_row in tables:
            table_name = table_row.tableName
            full_name = f"`{db}`.`{table_name}`"

            try:
                detail = s.sql(f"DESCRIBE DETAIL {full_name}").collect()
                location = detail[0]["location"] if detail else None
            except Exception as e:
                context.log.warning(f"  {full_name}: could not DESCRIBE DETAIL: {e}")
                results["tables"]["errors"].append(
                    {"table": full_name, "error": str(e)}
                )
                continue

            if not location or not location.startswith(old_prefix):
                context.log.info(
                    f"  {full_name}: location does not match, skipping ({(location or '')[:80]})"
                )
                results["tables"]["skipped"].append(full_name)
                continue

            new_location = location.replace(old_prefix, new_prefix, 1)
            context.log.info(f"  {full_name}: updating location")
            context.log.info(f"    old: {location}")
            context.log.info(f"    new: {new_location}")

            try:
                # Update SDS.LOCATION
                s.sql(f"ALTER TABLE {full_name} SET LOCATION '{new_location}'")
                # Update SERDE_PARAMS.path (Delta stores the path here too)
                s.sql(
                    f"ALTER TABLE {full_name} SET SERDEPROPERTIES ('path' = '{new_location}')"
                )
                results["tables"]["migrated"].append(
                    {"table": full_name, "old": location, "new": new_location}
                )
            except Exception as e:
                context.log.error(f"  {full_name}: ALTER TABLE failed: {e}")
                results["tables"]["errors"].append(
                    {"table": full_name, "error": str(e)}
                )

    return results


@asset
@capture_op_exceptions
def adhoc__migrate_hms_table_locations_to_abfss(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> Output[None]:
    """
    One-shot migration asset that updates all Hive Metastore location URIs
    from wasbs:// (Azure Blob Storage) to abfss:// (Azure Data Lake Storage Gen2).

    Covers all four HMS tables that store paths:
      - DBS.DB_LOCATION_URI        via ALTER DATABASE SET LOCATION
      - SDS.LOCATION               via ALTER TABLE SET LOCATION
      - SERDE_PARAMS (path key)    via ALTER TABLE SET SERDEPROPERTIES

    CTLGS.LOCATION_URI has no Spark SQL equivalent and must be updated manually:
      UPDATE "CTLGS"
      SET "LOCATION_URI" = replace("LOCATION_URI", '<old_prefix>', '<new_prefix>')
      WHERE "LOCATION_URI" LIKE '<old_prefix>%';

    Run this once after deploying the abfss:// storage migration. Once all
    locations are migrated and CTLGS is updated, the WASBS SAS credential
    in spark.py and metastore-site.template.xml can be removed.
    """
    s: SparkSession = spark.spark_session

    old_prefix = (
        f"wasbs://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_BLOB_SAS_HOST}"
    )
    new_prefix = (
        f"abfss://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_DFS_SAS_HOST}"
    )

    context.log.info(f"Migrating HMS locations: {old_prefix} -> {new_prefix}")

    results = _migrate_locations(context, s, old_prefix, new_prefix)

    db_errors = results["databases"]["errors"]
    tbl_errors = results["tables"]["errors"]

    context.log.info(
        f"Migration complete:\n"
        f"  Databases: {len(results['databases']['migrated'])} migrated, "
        f"{len(results['databases']['skipped'])} skipped, {len(db_errors)} errors\n"
        f"  Tables: {len(results['tables']['migrated'])} migrated, "
        f"{len(results['tables']['skipped'])} skipped, {len(tbl_errors)} errors"
    )

    if db_errors or tbl_errors:
        context.log.error(f"DB errors: {[e['db'] for e in db_errors]}")
        context.log.error(f"Table errors: {[e['table'] for e in tbl_errors]}")

    context.log.info(
        "CTLGS.LOCATION_URI must be updated manually via direct SQL on the HMS "
        "PostgreSQL database. See the asset docstring for the SQL statement."
    )

    return Output(
        None,
        metadata={
            "databases_migrated": len(results["databases"]["migrated"]),
            "databases_skipped": len(results["databases"]["skipped"]),
            "databases_errors": len(db_errors),
            "tables_migrated": len(results["tables"]["migrated"]),
            "tables_skipped": len(results["tables"]["skipped"]),
            "tables_errors": len(tbl_errors),
            "migrated_tables": [m["table"] for m in results["tables"]["migrated"]],
            "error_tables": [e["table"] for e in tbl_errors],
        },
    )
