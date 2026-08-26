from dagster_pyspark import PySparkResource
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from src.utils.schema import get_schema_columns
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, Output, asset

ERROR_TABLE_SCHEMA = "school_geolocation_error_table"
METASCHEMA = "school_geolocation"


@asset
@capture_op_exceptions
def adhoc__migrate_error_table_column_types(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> Output[None]:
    """One-shot migration fixing up error-table columns whose stored type has
    drifted from the metaschema.

    geolocation_error_table appends with mergeSchema, so each column kept the type
    it had on its first write, and never converges back to the metaschema when the
    write-side type changes later (e.g. duplicate_location_rows_count going from an
    uncast f.count("*") bigint to an int, or duplicate_group_id_50 going from a
    sequential int to an md5-hash string in #497). Delta only ever widens on merge,
    so any narrowing or type-family change has to be rewritten explicitly. This
    compares every column's stored type against the metaschema's declared type and
    casts whatever has drifted, in either direction.
    """
    s: SparkSession = spark.spark_session

    if not s.catalog.databaseExists(ERROR_TABLE_SCHEMA):
        context.log.info(f"Schema {ERROR_TABLE_SCHEMA} does not exist, nothing to do.")
        return Output(None)

    declared_types = {
        column.name: column.dataType for column in get_schema_columns(s, METASCHEMA)
    }
    context.log.info(f"{len(declared_types)} columns declared in {METASCHEMA}")

    migrated = []
    skipped = []
    errors = []

    for row in s.sql(f"SHOW TABLES IN {ERROR_TABLE_SCHEMA}").collect():
        full_name = f"{ERROR_TABLE_SCHEMA}.{row.tableName}"
        temp_name = f"{full_name}__type_migration"

        try:
            source = s.read.table(full_name)
            casts = {
                field.name: declared_types[field.name].simpleString()
                for field in source.schema.fields
                if field.name in declared_types
                and field.dataType != declared_types[field.name]
            }
        except Exception as exc:
            context.log.error(f"{full_name}: could not read: {exc}")
            errors.append({"table": full_name, "error": str(exc)})
            continue

        if not casts:
            context.log.info(f"{full_name}: nothing to migrate, skipping")
            skipped.append(full_name)
            continue

        context.log.info(f"{full_name}: casting {casts}")

        try:
            source_count = source.count()
            # Spark refuses to overwrite a table the same plan reads from.
            (
                source.withColumns(
                    {column: f.col(column).cast(cast) for column, cast in casts.items()}
                )
                .write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(temp_name)
            )

            staged = s.read.table(temp_name)
            staged_count = staged.count()
            if staged_count != source_count:
                raise ValueError(f"row count changed: {source_count} -> {staged_count}")

            (
                staged.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(full_name)
            )
            s.sql(f"DROP TABLE IF EXISTS {temp_name}")

            context.log.info(f"{full_name}: migrated {source_count} rows")
            migrated.append(full_name)
        except Exception as exc:
            context.log.error(
                f"{full_name}: migration failed: {exc}. "
                f"If {temp_name} exists it holds the converted rows."
            )
            errors.append({"table": full_name, "error": str(exc)})

    context.log.info(
        f"Migration complete: {len(migrated)} migrated, "
        f"{len(skipped)} skipped, {len(errors)} errors"
    )

    return Output(
        None,
        metadata={
            "migrated_count": len(migrated),
            "skipped_count": len(skipped),
            "error_count": len(errors),
            "migrated_tables": migrated,
            "error_tables": [e["table"] for e in errors],
        },
    )
