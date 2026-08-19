from dagster_pyspark import PySparkResource
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import IntegerType, LongType
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
    """One-shot migration narrowing bigint error-table columns to int.

    geolocation_error_table appends with mergeSchema, so each column kept the type
    of the first write. duplicate_location_rows_count came from an uncast
    f.count("*") and landed as bigint; now that the write runs through
    transform_types it is int, and Delta refuses the merge. Delta only ever widens,
    so the tables have to be rewritten.
    """
    s: SparkSession = spark.spark_session

    if not s.catalog.databaseExists(ERROR_TABLE_SCHEMA):
        context.log.info(f"Schema {ERROR_TABLE_SCHEMA} does not exist, nothing to do.")
        return Output(None)

    declared_int = {
        column.name
        for column in get_schema_columns(s, METASCHEMA)
        if isinstance(column.dataType, IntegerType)
    }
    context.log.info(f"{len(declared_int)} columns declared as int in {METASCHEMA}")

    migrated = []
    skipped = []
    errors = []

    for row in s.sql(f"SHOW TABLES IN {ERROR_TABLE_SCHEMA}").collect():
        full_name = f"{ERROR_TABLE_SCHEMA}.{row.tableName}"
        temp_name = f"{full_name}__type_migration"

        try:
            source = s.read.table(full_name)
            columns = [
                field.name
                for field in source.schema.fields
                if field.name in declared_int and isinstance(field.dataType, LongType)
            ]
        except Exception as exc:
            context.log.error(f"{full_name}: could not read: {exc}")
            errors.append({"table": full_name, "error": str(exc)})
            continue

        if not columns:
            context.log.info(f"{full_name}: nothing to narrow, skipping")
            skipped.append(full_name)
            continue

        context.log.info(f"{full_name}: narrowing {columns} to int")

        try:
            source_count = source.count()
            # Spark refuses to overwrite a table the same plan reads from.
            (
                source.withColumns(
                    {column: f.col(column).cast("int") for column in columns}
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
