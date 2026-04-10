from dagster_pyspark import PySparkResource
from src.utils.sentry import capture_op_exceptions

from dagster import Config, OpExecutionContext, Output, asset

from .migrate_hms_table_locations_to_abfss import _migrate_locations


class MigrateHmsLocationsConfig(Config):
    old_prefix: str
    new_prefix: str


@asset
@capture_op_exceptions
def adhoc__test_migrate_hms_table_locations(
    context: OpExecutionContext,
    config: MigrateHmsLocationsConfig,
    spark: PySparkResource,
) -> Output[None]:
    """
    Configurable version of the HMS table location migration asset for testing.

    Replaces old_prefix with new_prefix across all four HMS path tables:
      - DBS.DB_LOCATION_URI        via ALTER DATABASE SET LOCATION
      - SDS.LOCATION               via ALTER TABLE SET LOCATION
      - SERDE_PARAMS (path key)    via ALTER TABLE SET SERDEPROPERTIES

    CTLGS.LOCATION_URI is not handled here — update it manually via direct SQL.

    To undo: swap old_prefix and new_prefix and run again.
    """
    s = spark.spark_session

    context.log.info(
        f"Migrating HMS locations:\n  old: {config.old_prefix}\n  new: {config.new_prefix}"
    )

    results = _migrate_locations(context, s, config.old_prefix, config.new_prefix)

    db_errors = results["databases"]["errors"]
    tbl_errors = results["tables"]["errors"]

    context.log.info(
        f"Done:\n"
        f"  Databases: {len(results['databases']['migrated'])} migrated, "
        f"{len(results['databases']['skipped'])} skipped, {len(db_errors)} errors\n"
        f"  Tables: {len(results['tables']['migrated'])} migrated, "
        f"{len(results['tables']['skipped'])} skipped, {len(tbl_errors)} errors"
    )

    if db_errors or tbl_errors:
        context.log.error(f"DB errors: {[e['db'] for e in db_errors]}")
        context.log.error(f"Table errors: {[e['table'] for e in tbl_errors]}")

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
