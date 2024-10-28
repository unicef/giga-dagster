from delta.tables import DeltaMergeBuilder, DeltaTable, DeltaTableBuilder
from icecream import ic
from pyspark import sql
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.functions import collect_list, concat_ws, sha2
from pyspark.sql.types import DataType, StructField, StructType

from dagster import InputContext, OpExecutionContext, OutputContext
from src.constants import DataTier
from src.exceptions import MutexException
from src.settings import settings
from src.utils.schema import construct_full_table_name, construct_schema_name_for_tier


def execute_query_with_error_handler(
    spark: SparkSession,
    query: DeltaTableBuilder | DeltaMergeBuilder,
    schema_name: str,
    table_name: str,
    context: InputContext | OutputContext | OpExecutionContext,
) -> None:
    full_table_name = f"{schema_name}.{table_name}"

    try:
        query.execute()
    except AnalysisException as exc:
        if "DELTA_TABLE_NOT_FOUND" in str(exc):
            # This error gets raised when you delete the Delta Table in ADLS and subsequently try to re-ingest the
            # same table. Its corresponding entry in the metastore needs to be dropped first.
            #
            # Deleting a table in ADLS does not drop its metastore entry; the inverse is also true.
            context.log.warning(
                f"Attempting to drop metastore entry for `{full_table_name}`...",
            )
            spark.sql(f"DROP TABLE `{schema_name}`.`{table_name.lower()}`")
            query.execute()
            context.log.info("ok")
        else:
            raise exc


def create_delta_table(
    spark: SparkSession,
    schema_name: str,
    table_name: str,
    columns: StructType | list[StructField],
    context: InputContext | OutputContext | OpExecutionContext,
    *,
    if_not_exists: bool = False,
    replace: bool = False,
) -> None:
    if if_not_exists and replace:
        raise MutexException(
            "Only one of `if_not_exists` or `replace` can be set to True.",
        )

    full_table_name = construct_full_table_name(schema_name, table_name)
    create_stmt = DeltaTable.create

    if if_not_exists:
        create_stmt = DeltaTable.createIfNotExists
    if replace:
        create_stmt = DeltaTable.createOrReplace

    query = (
        create_stmt(spark)
        .tableName(full_table_name)
        .addColumns(columns)
        .property("delta.enableChangeDataFeed", "true")
    )
    execute_query_with_error_handler(spark, query, schema_name, table_name, context)


def create_schema(spark: SparkSession, schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")


def build_deduped_merge_query(
    master: DeltaTable,
    updates: sql.DataFrame,
    primary_key: str,
    update_columns: list[str],
    *,
    context: OpExecutionContext | OutputContext = None,
    is_partial_dataset=False,
    is_qos=False,
) -> DeltaMergeBuilder | None:
    """
    Delta Lake increments the dataset version and generates a change log when performing
    a merge, regardless of whether there were actually any changes. We perform a basic
    signature check to determine if there were actually any changes and perform the
    relevant merge operations only if there is at least one change for that particular
    operation (i.e. insert, update, delete).

    IMPORTANT: Because of this, it is crucial that you do not pass in partial datasets for the
    `updates` DataFrame without explicitly setting the `is_partial_dataset` flag. Otherwise,
    you may end up deleting more rows than you intended.
    """
    master_df = master.toDF()
    incoming = updates.alias("incoming")

    if is_qos:
        incoming_partitions = [
            r.date for r in incoming.select(f.col("date")).distinct().collect()
        ]
        bc_incoming_partitions = incoming.sparkSession.sparkContext.broadcast(
            incoming_partitions
        )
        master_df = master_df.filter(f.col("date").isin(bc_incoming_partitions.value))
        merge_condition = (
            f.col(f"master.{primary_key}") == f.col(f"incoming.{primary_key}")
        ) & (f.col("incoming.date").isin(bc_incoming_partitions.value))
    else:
        merge_condition = f.col(f"master.{primary_key}") == f.col(
            f"incoming.{primary_key}"
        )

    master_ids = master_df.select(primary_key, "signature")
    incoming_ids = incoming.select(primary_key, "signature")

    updates_df = incoming_ids.join(master_ids, primary_key, "inner")
    inserts_df = incoming_ids.join(master_ids, primary_key, "left_anti")
    deletes_df = master_ids.join(incoming_ids, primary_key, "left_anti")

    # Might need to specify a predictable order, although by default it's insertion order
    updates_signature = updates_df.agg(
        sha2(concat_ws("|", collect_list("incoming.signature")), 256).alias(
            "combined_signature",
        ),
    ).first()["combined_signature"]
    master_to_update_signature = master_ids.agg(
        sha2(concat_ws("|", collect_list("signature")), 256).alias(
            "combined_signature",
        ),
    ).first()["combined_signature"]

    inserts_count = inserts_df.count()
    deletes_count = deletes_df.count()

    has_updates = master_to_update_signature != updates_signature
    has_insertions = inserts_count > 0
    has_deletions = deletes_count > 0

    if not any(
        [has_updates, has_insertions, (has_deletions and not is_partial_dataset)]
    ):
        return None

    if context is not None:
        context.log.info(f"{inserts_count=}, {deletes_count=}, {has_updates=}")

    query = master.alias("master").merge(incoming.alias("incoming"), merge_condition)

    if has_updates:
        query = query.whenMatchedUpdate(
            "master.signature <> incoming.signature",
            dict(
                zip(
                    update_columns,
                    [f"incoming.{c}" for c in update_columns],
                    strict=True,
                ),
            ),
        )
    if has_insertions:
        query = query.whenNotMatchedInsertAll()
    if ic(has_deletions) and not is_partial_dataset:
        query = query.whenNotMatchedBySourceDelete()

    return query


def build_deduped_delete_query(
    master: DeltaTable,
    delete_ids: list[str],
    primary_key: str,
) -> DeltaMergeBuilder | None:
    master_df = master.toDF()

    deletes_df = master_df.filter(master_df[primary_key].isin(delete_ids))

    has_deletes = deletes_df.count() > 0

    if not (ic(has_deletes)):
        return None

    query = master.alias("master").merge(
        deletes_df.alias("deletes"),
        f"master.{primary_key} = deletes.{primary_key}",
    )

    if has_deletes:
        query = query.whenMatchedDelete()

    return query


def get_change_operation_counts(df: sql.DataFrame):
    counts = df.groupBy("_change_type").agg(f.count("*").alias("count")).collect()

    def get_count_by_change_type(change_type: str):
        return next((c["count"] for c in counts if c["_change_type"] == change_type), 0)

    return {
        "added": get_count_by_change_type("insert"),
        "modified": get_count_by_change_type("update_postimage"),
        "deleted": get_count_by_change_type("delete"),
    }


def check_table_exists(
    spark: SparkSession, schema_name: str, table_name: str, data_tier: DataTier = None
) -> bool:
    tiered_schema_name = construct_schema_name_for_tier(
        schema_name,
        data_tier,
    )
    table_path = ic(
        f"{settings.SPARK_WAREHOUSE_DIR}/{tiered_schema_name}.db/{table_name.lower()}"
    )

    return ic(spark.catalog.tableExists(table_name, tiered_schema_name)) and ic(
        DeltaTable.isDeltaTable(spark, table_path)
    )


def get_changed_datatypes(
    context: OpExecutionContext, existing_schema: StructType, updated_schema: StructType
) -> dict[str, DataType]:
    original_datatypes = {}
    changed_datatypes = {}
    context.log.info(f"Existing schema {existing_schema}")
    context.log.info(f"Updated schema {updated_schema}")

    for column in existing_schema:
        if (
            match_ := next((c for c in updated_schema if c.name == column.name), None)
        ) is not None:
            if match_.dataType != column.dataType:
                changed_datatypes[match_.name] = match_.dataType
                original_datatypes[match_.name] = column.dataType

    context.log.info(f"Original datatypes: {original_datatypes}")
    context.log.info(f"Changed datatypes: {changed_datatypes}")
    return changed_datatypes


def build_nullability_queries(
    context: OpExecutionContext,
    existing_schema: StructType,
    updated_schema: StructType,
    table_name: str,
) -> list[str]:
    context.log.info("Building nullability queries...")
    context.log.info(f"Existing schema {existing_schema}")
    context.log.info(f"Updated schema {updated_schema}")
    alter_stmts = []
    for column in existing_schema:
        if (
            match_ := next((c for c in updated_schema if c.name == column.name), None)
        ) is not None:
            if match_.nullable != column.nullable:
                if match_.nullable:
                    alter_stmts.append(
                        f"ALTER TABLE {table_name} ALTER COLUMN {column.name} DROP NOT NULL"
                    )

                else:
                    alter_stmts.append(
                        f"ALTER TABLE {table_name} ALTER COLUMN {column.name} SET NOT NULL"
                    )

    return alter_stmts


def sync_schema(
    table_name: str,
    existing_schema: StructType,
    updated_schema: StructType,
    spark: SparkSession,
    context: OpExecutionContext,
):
    alter_stmts = build_nullability_queries(
        context=context,
        existing_schema=existing_schema,
        updated_schema=updated_schema,
        table_name=table_name,
    )
    context.log.info(f"alter_stmts {alter_stmts}")
    has_nullability_changed = len(alter_stmts) > 0

    updated_columns = sorted(updated_schema.fieldNames())
    existing_columns = sorted(existing_schema.fieldNames())
    has_schema_changed = updated_columns != existing_columns

    changed_datatypes = get_changed_datatypes(
        context=context, existing_schema=existing_schema, updated_schema=updated_schema
    )
    has_datatype_changed = len(changed_datatypes) > 0

    context.log.info(f"has_datatype_changed {has_datatype_changed}")
    if has_datatype_changed:
        context.log.info("Updating datatype...")
        existing_dataframe = spark.table(table_name)
        updated_df = existing_dataframe

        for column, datatype in changed_datatypes.items():
            updated_df = updated_df.withColumn(
                column, existing_dataframe[column].cast(datatype.typeName())
            )

        (
            updated_df.write.option("overwriteSchema", "true")
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

    context.log.info(f"has_schema_changed {has_schema_changed}")

    if has_schema_changed:
        context.log.info("Adding schema columns...")
        empty_dataframe_with_updated_schema = spark.createDataFrame(
            [], schema=updated_schema
        )
        (
            empty_dataframe_with_updated_schema.write.option("mergeSchema", "true")
            .format("delta")
            .mode("append")
            .saveAsTable(table_name)
        )
    context.log.info(f"has_nullability_changed {has_nullability_changed}")

    if has_nullability_changed:
        context.log.info(
            f"Modifying column nullabilities with the SQL statements{alter_stmts}..."
        )

        for stmnt in alter_stmts:
            context.log.info(f"executing sql: {stmnt}")
            spark.sql(stmnt).show()
