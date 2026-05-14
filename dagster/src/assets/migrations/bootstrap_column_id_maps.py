"""One-time migration: bootstrap giga.columnId.* table properties for all existing
Delta tables that predate the UUID-based rename/delete detection feature.

For tables where the schema CSV has already been renamed before this feature was
deployed, the script also detects the mismatch and renames the physical column to
match the current schema before persisting the mapping — eliminating the need for
manual per-table ALTER TABLE statements.

Run once per environment after deploying the rename/delete detection feature.
Safe to re-run: tables that already have the mapping stored are skipped.
"""

from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from src.constants import DataTier
from src.utils.delta import (
    enable_column_mapping,
    get_stored_column_id_map,
    persist_column_id_map,
)
from src.utils.schema import (
    construct_schema_name_for_tier,
    get_schema_columns_with_id,
)

from dagster import OpExecutionContext, asset


def _get_all_country_tables(
    spark: SparkSession,
    tier_schema: str,
) -> list[str]:
    """Return all fully-qualified table names in a given schema."""
    if not spark.catalog.databaseExists(tier_schema):
        return []
    rows = spark.sql(f"SHOW TABLES IN `{tier_schema}`").collect()
    return [f"{tier_schema}.{row['tableName']}" for row in rows]


def _resolve_schema_name(dataset_type: str) -> str:
    """Return the schemas-namespace table name for a dataset type, e.g. 'school_geolocation'."""
    return dataset_type


def bootstrap_table(
    spark: SparkSession,
    context: OpExecutionContext,
    full_table_name: str,
    schema_name: str,
    dry_run: bool = False,
) -> None:
    """Bootstrap or repair the column-ID mapping for a single Delta table.

    Steps:
    1. If the table already has a complete mapping, skip it.
    2. Build updated_id_map from the schema CSV (name -> uuid).
    3. For physical columns that have no stored UUID and no direct name match in
       the schema, try to match them to unmatched schema columns by comparing
       the set of columns that differ between table and schema.
       If the mismatch is unambiguous (1 old name : 1 new name), rename the
       physical column and proceed.  If ambiguous, log a warning and skip.
    4. Call persist_column_id_map to store the final mapping.
    """
    if not spark.catalog.tableExists(full_table_name):
        context.log.info(f"Table {full_table_name} does not exist — skipping.")
        return

    existing_map = get_stored_column_id_map(spark, full_table_name)

    try:
        columns_with_id = get_schema_columns_with_id(spark, schema_name)
    except Exception as exc:
        context.log.warning(
            f"Could not load schema '{schema_name}' for {full_table_name}: {exc} — skipping."
        )
        return

    updated_id_map: dict[str, str] = {
        field.name: csv_id for csv_id, field in columns_with_id
    }

    spark.catalog.refreshTable(full_table_name)
    physical_columns: list[str] = spark.table(full_table_name).columns

    # Columns in the table that have no stored UUID and no direct name match in schema
    orphan_table_cols = [
        c for c in physical_columns if c not in existing_map and c not in updated_id_map
    ]
    # Schema columns that are not physically present in the table
    missing_schema_cols = [c for c in updated_id_map if c not in physical_columns]

    if orphan_table_cols:
        context.log.info(
            f"{full_table_name}: orphan table columns (no UUID, no schema match): "
            f"{orphan_table_cols}"
        )
        context.log.info(
            f"{full_table_name}: missing schema columns (in schema but not in table): "
            f"{missing_schema_cols}"
        )

    if orphan_table_cols and missing_schema_cols:
        if len(orphan_table_cols) == len(missing_schema_cols):
            # Unambiguous 1-to-1 mismatch — safe to rename
            renames = dict(zip(orphan_table_cols, missing_schema_cols, strict=False))
            context.log.info(f"{full_table_name}: detected probable renames: {renames}")
            if not dry_run:
                enable_column_mapping(spark, full_table_name)
                for old_name, new_name in renames.items():
                    stmt = (
                        f"ALTER TABLE {full_table_name} "
                        f"RENAME COLUMN `{old_name}` TO `{new_name}`"
                    )
                    context.log.info(f"Executing: {stmt}")
                    spark.sql(stmt)
                spark.catalog.refreshTable(full_table_name)
            else:
                context.log.info(
                    f"[DRY RUN] Would rename columns in {full_table_name}: {renames}"
                )
        else:
            # Ambiguous — multiple columns differ; cannot safely auto-rename
            context.log.warning(
                f"{full_table_name}: ambiguous column mismatch "
                f"(orphan table cols: {orphan_table_cols}, "
                f"missing schema cols: {missing_schema_cols}). "
                f"Cannot auto-rename. Manual intervention required."
            )
            # Still persist what we can so the rest of the mapping is stored
    elif not orphan_table_cols and not missing_schema_cols:
        context.log.info(
            f"{full_table_name}: all physical columns match the schema by name."
        )

    if not dry_run:
        persist_column_id_map(spark, full_table_name, schema_name)
        context.log.info(f"{full_table_name}: column-ID mapping persisted.")
    else:
        context.log.info(
            f"[DRY RUN] Would persist column-ID mapping for {full_table_name}."
        )


@asset
def bootstrap_column_id_maps(
    context: OpExecutionContext,
    spark: PySparkResource,
) -> None:
    """One-time asset: bootstrap giga.columnId.* props for all existing Delta tables.

    Set DRY_RUN = True to log what would happen without making any changes.
    """
    DRY_RUN = False

    s: SparkSession = spark.spark_session

    # Map of (dataset_type, tier) -> schema_name used for rename/delete detection
    # Add more entries here if other dataset types are introduced.
    dataset_configs: list[tuple[str, DataTier | None]] = [
        ("school_geolocation", DataTier.SILVER),
        ("school_geolocation", DataTier.STAGING),
        ("school_geolocation", None),  # master: schema = "school_master"
    ]

    for dataset_type, tier in dataset_configs:
        if tier is None:
            tier_schema = "school_master"
            schema_name = dataset_type
        else:
            tier_schema = construct_schema_name_for_tier(
                f"school_{dataset_type.replace('school_', '')}", tier
            )
            schema_name = dataset_type

        context.log.info(
            f"Processing schema '{tier_schema}' with schema_name='{schema_name}' ..."
        )
        table_names = _get_all_country_tables(s, tier_schema)

        if not table_names:
            context.log.info(f"No tables found in '{tier_schema}' — skipping.")
            continue

        for full_table_name in table_names:
            bootstrap_table(
                spark=s,
                context=context,
                full_table_name=full_table_name,
                schema_name=schema_name,
                dry_run=DRY_RUN,
            )
