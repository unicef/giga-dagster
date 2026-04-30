from delta.tables import DeltaMergeBuilder, DeltaTable, DeltaTableBuilder
from icecream import ic
from pyspark import sql
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import (
    SparkSession,
    functions as f,
)
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
    partition_by: list[str] | None = None,
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
    if partition_by:
        query = query.partitionedBy(*partition_by)
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
    incoming = updates.alias("incoming")

    if is_qos:
        incoming_partitions = [
            r.date for r in incoming.select(f.col("date")).distinct().collect()
        ]

        if context is not None:
            context.log.info(
                f"Processing {len(incoming_partitions)} date partitions: {incoming_partitions}"
            )

        partition_filter = f.col("date").isin(incoming_partitions)

        master_df = master.toDF().filter(partition_filter)
        merge_condition = f.col(f"master.{primary_key}") == f.col(
            f"incoming.{primary_key}"
        )

        bc_incoming_partitions = incoming.sparkSession.sparkContext.broadcast(
            incoming_partitions
        )
        merge_condition = merge_condition & f.col("master.date").isin(
            bc_incoming_partitions.value
        )
    else:
        master_df = master.toDF()
        merge_condition = f.col(f"master.{primary_key}") == f.col(
            f"incoming.{primary_key}"
        )

    master_ids = master_df.select(
        primary_key, f.col("signature").alias("master_signature")
    )
    incoming_ids = incoming.select(
        primary_key, f.col("signature").alias("incoming_signature")
    )

    updates_df = incoming_ids.join(master_ids, primary_key, "inner")
    inserts_df = incoming_ids.join(master_ids, primary_key, "left_anti")
    deletes_df = master_df.select(primary_key).join(
        incoming_ids, primary_key, "left_anti"
    )

    has_updates = (
        updates_df.filter(f.col("master_signature") != f.col("incoming_signature"))
        .limit(1)
        .count()
    ) > 0
    inserts_count = inserts_df.count()
    deletes_count = deletes_df.count()

    has_insertions = inserts_count > 0
    has_deletions = deletes_count > 0

    if not any(
        [has_updates, has_insertions, (has_deletions and not is_partial_dataset)]
    ):
        return None

    if context is not None:
        context.log.info(f"{is_qos=}, {is_partial_dataset=}")
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
                alter_stmts.append(
                    f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {column.name}_not_null"
                )

                if match_.nullable:
                    alter_stmts.append(
                        f"ALTER TABLE {table_name} ALTER COLUMN {column.name} DROP NOT NULL"
                    )

                else:
                    alter_stmts.append(
                        f"ALTER TABLE {table_name} ADD CONSTRAINT {column.name}_not_null CHECK ({column.name} is not null)"
                    )

    return alter_stmts


def enable_column_mapping(spark: SparkSession, table_name: str) -> None:
    """Enable column mapping mode on an existing Delta table if not already enabled."""
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
        f"  'delta.columnMapping.mode' = 'name',"
        f"  'delta.minReaderVersion' = '2',"
        f"  'delta.minWriterVersion' = '5'"
        f")"
    )


def get_stored_column_id_map(spark: SparkSession, table_name: str) -> dict[str, str]:
    """Retrieve the column-name → schema-CSV-ID mapping stored in table properties.

    Returns ``{column_name: csv_id}`` or an empty dict if no mapping has been
    stored yet (e.g. tables created before this feature was added).
    """
    spark.catalog.refreshTable(table_name)
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    properties: dict = detail["properties"] if detail["properties"] else {}
    result = {}
    prefix = "giga.columnId."
    for key, value in properties.items():
        if key.startswith(prefix):
            col_name = key[len(prefix) :]
            result[col_name] = value
    return result


def store_column_id_map(
    spark: SparkSession,
    table_name: str,
    column_id_map: dict[str, str],
) -> None:
    """Persist the column-name → schema-CSV-ID mapping as Delta table properties.

    Also removes any stale ``giga.columnId.*`` properties for columns that are
    not present in the new mapping.  This prevents accumulation of old column
    name props across renames, which would otherwise cause future rename
    detection to misbehave (e.g. multiple props pointing to the same UUID).
    """
    if not column_id_map:
        return

    # Remove stale props for columns that no longer exist in the new mapping
    current_props = get_stored_column_id_map(spark, table_name)
    stale_columns = [
        col_name for col_name in current_props if col_name not in column_id_map
    ]
    if stale_columns:
        remove_column_id_props(spark, table_name, stale_columns)

    props = ", ".join(
        f"'giga.columnId.{col_name}' = '{csv_id}'"
        for col_name, csv_id in column_id_map.items()
    )
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props})")


def remove_column_id_props(
    spark: SparkSession,
    table_name: str,
    column_names: list[str],
) -> None:
    """Remove column-ID table properties for dropped columns."""
    if not column_names:
        return
    props = ", ".join(f"'giga.columnId.{name}'" for name in column_names)
    spark.sql(f"ALTER TABLE {table_name} UNSET TBLPROPERTIES IF EXISTS ({props})")


def detect_renames_and_deletes(
    existing_id_map: dict[str, str],
    updated_id_map: dict[str, str],
) -> tuple[dict[str, str], list[str]]:
    """Compare old and new column-ID mappings to detect renames and deletes.

    Parameters
    ----------
    existing_id_map : dict[str, str]
        ``{column_name: csv_id}`` from the current table properties.
    updated_id_map : dict[str, str]
        ``{column_name: csv_id}`` from the latest schema CSV.

    Returns
    -------
    renames : dict[str, str]
        ``{old_name: new_name}`` for columns whose ID stayed but name changed.
    deletes : list[str]
        Column names present in the table but whose ID is no longer in the
        updated schema (i.e. the column should be dropped).
    """
    # Invert maps:  csv_id → column_name
    existing_by_id = {v: k for k, v in existing_id_map.items()}
    updated_by_id = {v: k for k, v in updated_id_map.items()}

    renames: dict[str, str] = {}
    deletes: list[str] = []

    for csv_id, old_name in existing_by_id.items():
        if csv_id in updated_by_id:
            new_name = updated_by_id[csv_id]
            if old_name != new_name:
                renames[old_name] = new_name
        else:
            # ID no longer present in the reference schema → column deleted
            deletes.append(old_name)

    return renames, deletes


def initialize_column_id_map(
    spark: SparkSession,
    table_name: str,
    updated_id_map: dict[str, str],
    context: OpExecutionContext,
) -> tuple[dict[str, str], dict[str, str], list[str]]:
    """Initialize column ID mapping from table schema when no stored mapping exists.

    Returns tuple of (initialized_id_map, renames, deletes).
    """
    context.log.info(
        "No stored column-ID mapping found; initialising mapping from current table schema."
    )
    existing_id_map: dict[str, str] = {}
    # Initialize mapping from current table columns so deletions can be detected
    # This handles the case where columns were dropped from CSV but table still has them
    current_columns = DeltaTable.forName(spark, table_name).toDF().schema.fieldNames()
    for col_name in current_columns:
        if col_name not in updated_id_map:
            # Column exists in table but not in reference schema - will be detected as delete
            # Use a deterministic ID based on column name for tracking
            existing_id_map[col_name] = f"table_{col_name}"

    renames: dict[str, str] = {}
    deletes: list[str] = []
    if existing_id_map:
        renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)
        context.log.info(f"Detected renames after init: {renames}")
        context.log.info(f"Detected deletes after init: {deletes}")

    return existing_id_map, renames, deletes


def execute_renames(
    spark: SparkSession,
    table_name: str,
    renames: dict[str, str],
    context: OpExecutionContext,
) -> None:
    """Execute column rename SQL statements."""
    context.log.info(f"Renaming columns: {renames}")
    for old_name, new_name in renames.items():
        stmt = f"ALTER TABLE {table_name} RENAME COLUMN `{old_name}` TO `{new_name}`"
        context.log.info(f"Executing: {stmt}")
        spark.sql(stmt)
    remove_column_id_props(spark, table_name, list(renames.keys()))


def execute_deletes(
    spark: SparkSession,
    table_name: str,
    deletes: list[str],
    context: OpExecutionContext,
) -> None:
    """Execute column drop SQL statements."""
    context.log.info(f"Dropping columns: {deletes}")
    for col_name in deletes:
        stmt = f"ALTER TABLE {table_name} DROP COLUMN `{col_name}`"
        context.log.info(f"Executing: {stmt}")
        spark.sql(stmt)
    remove_column_id_props(spark, table_name, deletes)


def apply_renames_and_deletes(
    spark: SparkSession,
    table_name: str,
    schema_name: str,
    context: OpExecutionContext,
) -> bool:
    """Detect and apply column renames and deletes to a Delta table based on the reference schema.

    Returns True if any schema change occurred (rename or delete).

    IMPORTANT: This function ALWAYS supplements ``existing_id_map`` with any
    table columns that lack stored UUIDs.  This prevents the situation where a
    partially-populated ``column_id_map`` causes some columns to be silently
    ignored by rename/delete detection (which previously led to those columns
    being dropped by the fallback path in :func:`sync_schema`, causing data
    loss).
    """
    from src.utils.schema import get_schema_columns_with_id

    columns_with_id = get_schema_columns_with_id(spark, schema_name)
    updated_id_map = {field.name: csv_id for csv_id, field in columns_with_id}
    existing_id_map = get_stored_column_id_map(spark, table_name)

    # If the table has NO stored UUIDs at all (pre-existing table that was
    # created before UUID-based tracking was introduced), bootstrapping is
    # needed first.  We must NOT treat all columns as deletes — that would
    # cause data loss by dropping every column.  Instead, persist the current
    # mapping now and skip rename/delete detection for this run.
    if not existing_id_map:
        context.log.info(
            f"No stored column-ID mapping found for {table_name}. "
            "Bootstrapping UUID props from current schema. "
            "Rename/delete detection will be active from the next run onwards."
        )
        persist_column_id_map(spark, table_name, schema_name)
        return False

    # Supplement existing_id_map with table columns that lack stored UUIDs.
    # This handles columns added via mergeSchema after the initial bootstrap
    # (e.g. ADD operations that ran before persist_column_id_map was called).
    # Tag them with a synthetic ID so they are treated as deletes if they are
    # no longer in the reference schema.
    current_columns = DeltaTable.forName(spark, table_name).toDF().schema.fieldNames()
    for col_name in current_columns:
        if col_name not in existing_id_map:
            existing_id_map[col_name] = f"table_{col_name}"
            context.log.info(
                f"Column '{col_name}' exists in table but has no stored UUID; "
                f"tagging with synthetic ID for delete detection."
            )

    renames, deletes = detect_renames_and_deletes(existing_id_map, updated_id_map)
    context.log.info(f"Detected renames: {renames}")
    context.log.info(f"Detected deletes: {deletes}")

    if renames or deletes:
        context.log.info(
            "Enabling column mapping on table for rename/delete support..."
        )
        enable_column_mapping(spark, table_name)

    if renames:
        execute_renames(spark, table_name, renames, context)

    if deletes:
        execute_deletes(spark, table_name, deletes, context)

    if renames or deletes:
        spark.catalog.refreshTable(table_name)

    return bool(renames or deletes)


def persist_column_id_map(
    spark: SparkSession, table_name: str, schema_name: str
) -> None:
    """Read the column ID mapping from the schema CSV and store it as table properties."""
    from src.utils.schema import get_schema_columns_with_id

    columns_with_id = get_schema_columns_with_id(spark, schema_name)
    new_id_map = {field.name: csv_id for csv_id, field in columns_with_id}
    store_column_id_map(spark, table_name, new_id_map)


def apply_datatype_changes(
    spark: SparkSession,
    table_name: str,
    changed_datatypes: dict,
    context: OpExecutionContext,
) -> None:
    """Apply datatype changes by casting columns and overwriting the table schema."""
    if not changed_datatypes:
        return

    context.log.info("Updating datatype...")
    context.log.info(f"Changed datatypes: {changed_datatypes}")
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
    spark.catalog.refreshTable(table_name)


def handle_removed_columns(
    spark: SparkSession,
    table_name: str,
    removed_columns: set[str],
    schema_name: str | None,
    context: OpExecutionContext,
) -> None:
    """Safely handle columns that exist in the table but not in the updated schema.

    When ``schema_name`` is provided, :func:`apply_renames_and_deletes` is the
    authoritative path for both renames and deletes (UUID-based detection).
    If columns end up here despite that, it likely means rename detection
    failed for them.  Log a clear warning instead of silently dropping to
    prevent data loss.

    When ``schema_name`` is None (legacy callers, schema-tables migration),
    fall back to dropping by name as before.
    """
    if not removed_columns:
        return

    if schema_name is not None:
        context.log.warning(
            f"Columns exist in table but not in updated schema: "
            f"{removed_columns}. These were NOT handled by "
            f"apply_renames_and_deletes - leaving them in place to avoid "
            f"unintended data loss. If you intend to drop them, remove the "
            f"column from the schema CSV (with its UUID) and re-run."
        )
        return

    context.log.info(f"Dropping columns not in updated schema: {removed_columns}")
    for col_name in removed_columns:
        stmt = f"ALTER TABLE {table_name} DROP COLUMN `{col_name}`"
        context.log.info(f"Executing: {stmt}")
        spark.sql(stmt)


def sync_schema(
    table_name: str,
    existing_schema: StructType,
    updated_schema: StructType,
    spark: SparkSession,
    context: OpExecutionContext,
    schema_name: str | None = None,
):
    """Synchronise a Delta table's schema with the reference schema.

    Supports:
    * Adding columns (existing behaviour via ``mergeSchema``)
    * Renaming columns (via ``ALTER TABLE RENAME COLUMN``)
    * Dropping columns (via ``ALTER TABLE DROP COLUMN``)
    * Changing data types (via overwrite with ``overwriteSchema``)
    * Changing nullability constraints

    Column renames and deletes require ``schema_name`` so that the stable
    UUID column IDs from the schema CSV can be compared against the IDs
    stored in the table properties.
    """
    # ------------------------------------------------------------------
    # 1. Detect and apply renames & deletes
    # ------------------------------------------------------------------
    any_renames_deletes = False
    if schema_name is not None:
        any_renames_deletes = apply_renames_and_deletes(
            spark, table_name, schema_name, context
        )

    # ------------------------------------------------------------------
    # 2. Refresh schemas after rename/delete to get accurate comparison
    # ------------------------------------------------------------------
    if any_renames_deletes:
        spark.catalog.refreshTable(table_name)
        existing_schema = spark.table(table_name).schema

    # ------------------------------------------------------------------
    # 3. Detect added columns & datatype changes (existing logic)
    # ------------------------------------------------------------------
    alter_stmts = build_nullability_queries(
        context=context,
        existing_schema=existing_schema,
        updated_schema=updated_schema,
        table_name=table_name,
    )
    context.log.info(f"alter_stmts {alter_stmts}")
    has_nullability_changed = len(alter_stmts) > 0

    existing_columns = {field.name for field in existing_schema}
    updated_columns_set = {field.name for field in updated_schema}

    added_columns = updated_columns_set - existing_columns
    # Recalculate removed_columns after renames/deletes were applied
    # This ensures we correctly identify columns that should be dropped
    removed_columns = existing_columns - updated_columns_set

    changed_datatypes = get_changed_datatypes(
        context=context, existing_schema=existing_schema, updated_schema=updated_schema
    )
    apply_datatype_changes(spark, table_name, changed_datatypes, context)

    if added_columns:
        context.log.info(f"Adding schema columns {added_columns}")

        empty_dataframe_with_updated_schema = spark.createDataFrame(
            [], schema=updated_schema
        )
        (
            empty_dataframe_with_updated_schema.write.option("mergeSchema", "true")
            .format("delta")
            .mode("append")
            .saveAsTable(table_name)
        )

    handle_removed_columns(spark, table_name, removed_columns, schema_name, context)

    context.log.info(f"has_nullability_changed {has_nullability_changed}")

    if has_nullability_changed:
        context.log.info(
            f"Modifying column nullabilities with the SQL statements{alter_stmts}..."
        )

        for stmnt in alter_stmts:
            context.log.info(f"executing sql: {stmnt}")
            try:
                spark.sql(stmnt).show()
            except AnalysisException as exc:
                if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(exc):
                    continue
                else:
                    raise
