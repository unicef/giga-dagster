from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from src.utils.nocodb.get_nocodb_data import (
    get_nocodb_table_as_multi_value_mapping,
    get_nocodb_table_id_from_name,
)
from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, Output, asset

TABLES_TO_MIGRATE = ["school_geolocation_silver", "school_master"]


@asset
@capture_op_exceptions
def adhoc__migrate_connectivity_type_mapping(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> Output[None]:
    """
    One-shot migration asset that remaps existing connectivity_type /
    connectivity_type_root values already stored in silver and master to the
    values defined by the NocoDB ConnectivityTypeMapping table, keyed on
    connectivity_type_old (the pre-migration connectivity_type value).

    Run this once after the ConnectivityTypeMapping table is live and the
    pipeline has switched to deriving connectivity_type/connectivity_type_root
    from it. Safe to delete this asset once run.
    """
    s: SparkSession = spark.spark_session

    table_id = get_nocodb_table_id_from_name("ConnectivityTypeMapping")
    mappings = get_nocodb_table_as_multi_value_mapping(
        table_id=table_id,
        key_column="connectivity_type_old",
        value_columns=["connectivity_type", "connectivity_type_root"],
    )
    old_to_type = mappings["connectivity_type"]
    old_to_root = mappings["connectivity_type_root"]
    if not (old_to_type and old_to_root):
        context.log.warning(
            "No connectivity_type_old mappings found in ConnectivityTypeMapping; nothing to migrate."
        )
        return Output(None, metadata={"updated_tables": []})

    old_values = list(old_to_type.keys())
    type_map = f.create_map([f.lit(x) for pair in old_to_type.items() for x in pair])
    root_map = f.create_map([f.lit(x) for pair in old_to_root.items() for x in pair])

    updated_tables = []
    errors = []
    for schema_name in TABLES_TO_MIGRATE:
        tables = [
            row.tableName for row in s.sql(f"SHOW TABLES IN `{schema_name}`").collect()
        ]
        context.log.info(f"`{schema_name}`: {len(tables)} tables")

        for table_name in tables:
            full_name = f"`{schema_name}`.`{table_name}`"
            try:
                DeltaTable.forName(s, full_name).update(
                    condition=f.col("connectivity_type").isin(old_values),
                    set={
                        "connectivity_type": type_map[f.col("connectivity_type")],
                        "connectivity_type_root": f.coalesce(
                            root_map[f.col("connectivity_type")],
                            f.col("connectivity_type_root"),
                        ),
                    },
                )
            except Exception as e:
                context.log.warning(f"  {full_name}: update failed, skipping: {e}")
                errors.append({"table": full_name, "error": str(e)})
                continue

            context.log.info(f"  {full_name}: update applied")
            updated_tables.append(full_name)

    if errors:
        context.log.error(f"Tables with errors: {[e['table'] for e in errors]}")

    return Output(
        None,
        metadata={
            "updated_tables": updated_tables,
            "error_count": len(errors),
            "error_tables": [e["table"] for e in errors],
        },
    )
