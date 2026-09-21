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
                (r["info_value"] for r in db_detail if r["info_name"] == "Location"),
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
                # DESCRIBE EXTENDED reads from the HMS catalog — much cheaper than
                # DESCRIBE DETAIL which scans the Delta log and OOMs on large tables.
                rows = s.sql(f"DESCRIBE EXTENDED {full_name}").collect()
                location = next(
                    (r["data_type"] for r in rows if r["col_name"] == "Location"),
                    None,
                )
            except Exception as e:
                context.log.warning(f"  {full_name}: could not DESCRIBE EXTENDED: {e}")
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
                s.sql(f"ALTER TABLE {full_name} SET LOCATION '{new_location}'")
            except Exception as e:
                context.log.error(
                    f"  {full_name}: ALTER TABLE SET LOCATION failed: {e}"
                )
                results["tables"]["errors"].append(
                    {"table": full_name, "error": str(e)}
                )
                continue

            try:
                s.sql(
                    f"ALTER TABLE {full_name} SET SERDEPROPERTIES ('path' = '{new_location}')"
                )
            except Exception as e:
                # v2 tables (Delta) do not support SET SERDEPROPERTIES; SET LOCATION above is sufficient
                context.log.warning(f"  {full_name}: SET SERDEPROPERTIES skipped: {e}")

            results["tables"]["migrated"].append(
                {"table": full_name, "old": location, "new": new_location}
            )

    return results


def _wasbs_prefix() -> str:
    return (
        f"wasbs://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_BLOB_SAS_HOST}"
    )


def _abfss_prefix() -> str:
    return f"abfss://{settings.AZURE_BLOB_CONTAINER_NAME}@{settings.AZURE_DFS_SAS_HOST}"


def _rewrite_locations(
    context: OpExecutionContext,
    spark: PySparkResource,
    old_prefix: str,
    new_prefix: str,
) -> Output[None]:
    s: SparkSession = spark.spark_session

    context.log.info(f"Rewriting HMS locations: {old_prefix} -> {new_prefix}")

    results = _migrate_locations(context, s, old_prefix, new_prefix)

    db_errors = results["databases"]["errors"]
    tbl_errors = results["tables"]["errors"]

    context.log.info(
        f"Complete:\n"
        f"  Databases: {len(results['databases']['migrated'])} rewritten, "
        f"{len(results['databases']['skipped'])} skipped, {len(db_errors)} errors\n"
        f"  Tables: {len(results['tables']['migrated'])} rewritten, "
        f"{len(results['tables']['skipped'])} skipped, {len(tbl_errors)} errors"
    )

    if db_errors or tbl_errors:
        context.log.error(f"DB errors: {[e['db'] for e in db_errors]}")
        context.log.error(f"Table errors: {[e['table'] for e in tbl_errors]}")

    context.log.info(
        "CTLGS.LOCATION_URI is not handled here — it is updated by the k8s jobs in "
        "infra/k8s (migrate-hms-ctlgs-job.yaml / rollback-hms-ctlgs-job.yaml)."
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


@asset
@capture_op_exceptions
def adhoc__migrate_hms_table_locations_to_abfss(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> Output[None]:
    """
    One-shot migration asset that updates all Hive Metastore location URIs
    from wasbs:// (Azure Blob Storage) to abfss:// (Azure Data Lake Storage Gen2).

    Covers the HMS path tables reachable from Spark SQL:
      - DBS.DB_LOCATION_URI        via ALTER DATABASE SET LOCATION
      - SDS.LOCATION               via ALTER TABLE SET LOCATION
      - SERDE_PARAMS (path key)    via ALTER TABLE SET SERDEPROPERTIES

    CTLGS.LOCATION_URI has no Spark SQL equivalent; it is updated by the k8s job
    infra/k8s/migrate-hms-ctlgs-job.yaml, which runs during helm deploy.

    Run this once after deploying the abfss:// storage migration. Once all
    locations are migrated and CTLGS is updated, the WASBS SAS credential
    in spark.py and metastore-site.template.xml can be removed.

    To undo, run adhoc__rollback_hms_table_locations_to_wasbs.
    """
    return _rewrite_locations(context, spark, _wasbs_prefix(), _abfss_prefix())


@asset
@capture_op_exceptions
def adhoc__rollback_hms_table_locations_to_wasbs(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> Output[None]:
    """
    Reverse of adhoc__migrate_hms_table_locations_to_abfss: rewrites Hive
    Metastore location URIs from abfss:// back to wasbs://.

    CTLGS.LOCATION_URI is rolled back separately by the k8s job
    infra/k8s/rollback-hms-ctlgs-job.yaml, which must be applied manually.

    Safe to run against a partially migrated metastore — entries that do not
    start with the abfss:// prefix are skipped.
    """
    return _rewrite_locations(context, spark, _abfss_prefix(), _wasbs_prefix())
