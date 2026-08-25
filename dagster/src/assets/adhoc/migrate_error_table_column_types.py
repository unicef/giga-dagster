from dagster_pyspark import PySparkResource
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import IntegerType, LongType, StringType
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
    of the first write. Two drifts have shown up so far:
    - duplicate_location_rows_count came from an uncast f.count("*") and landed as
      bigint; the write now runs through transform_types and is int.
    - duplicate_group_id_50 used to be a sequential int; it's now an md5-hash
      string (#497), so old rows still carry int/bigint values.
    Delta only ever widens, and neither of these is a widening change, so the
    tables have to be rewritten.
    """
    s: SparkSession = spark.spark_session

    if not s.catalog.databaseExists(ERROR_TABLE_SCHEMA):
        context.log.info(f"Schema {ERROR_TABLE_SCHEMA} does not exist, nothing to do.")
        return Output(None)

    declared_int = set()
    declared_string = set()
    for column in get_schema_columns(s, METASCHEMA):
        if isinstance(column.dataType, IntegerType):
            declared_int.add(column.name)
        elif isinstance(column.dataType, StringType):
            declared_string.add(column.name)
    context.log.info(
        f"{len(declared_int)} columns declared as int, "
        f"{len(declared_string)} declared as string in {METASCHEMA}"
    )

    migrated = []
    skipped = []
    errors = []

    for row in s.sql(f"SHOW TABLES IN {ERROR_TABLE_SCHEMA}").collect():
        full_name = f"{ERROR_TABLE_SCHEMA}.{row.tableName}"
        temp_name = f"{full_name}__type_migration"

        try:
            source = s.read.table(full_name)
            narrow_to_int = [
                field.name
                for field in source.schema.fields
                if field.name in declared_int and isinstance(field.dataType, LongType)
            ]
            cast_to_string = [
                field.name
                for field in source.schema.fields
                if field.name in declared_string
                and isinstance(field.dataType, IntegerType | LongType)
            ]
        except Exception as exc:
            context.log.error(f"{full_name}: could not read: {exc}")
            errors.append({"table": full_name, "error": str(exc)})
            continue

        if not narrow_to_int and not cast_to_string:
            context.log.info(f"{full_name}: nothing to migrate, skipping")
            skipped.append(full_name)
            continue

        casts = {column: "int" for column in narrow_to_int}
        casts.update({column: "string" for column in cast_to_string})
        context.log.info(
            f"{full_name}: narrowing {narrow_to_int} to int, "
            f"casting {cast_to_string} to string"
        )

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
