from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from dagster import OpExecutionContext
from src.utils.logger import get_context_with_fallback_logger


def manual_review_dedupe_strat(df: sql.DataFrame):
    """
    In case multiple rows with the same `school_id_giga` are present,
    get only the row of the latest version.

    The case where there is more than one `_commit_version` for the same `school_id_giga`
    is for the `update_preimage`/`update_postimage` pair. Order by _change_type so that
    `update_postimage` comes first.
    """
    return (
        df.withColumn(
            "row_number",
            f.row_number().over(
                Window.partitionBy("school_id_giga").orderBy(
                    f.col("_commit_version").desc(),
                    f.col("_change_type"),
                )
            ),
        )
        .filter(f.col("row_number") == 1)
        .drop("row_number")
    )


def post_manual_review_merge(
    silver: sql.DataFrame,
    manual_reviewed: sql.DataFrame,
    column_names: list[str],
    primary_key: str,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)

    column_names_no_pk = [*column_names]
    if primary_key in column_names_no_pk:
        column_names_no_pk.remove(primary_key)

    # Extract operations; likely these will be partial tables.
    # Select only update_postimage since this is the only one we show in
    # the approve rows workflow.
    inserts = manual_reviewed.filter(f.col("_change_type") == "insert").select(
        *column_names
    )
    updates = manual_reviewed.filter(
        f.col("_change_type") == "update_postimage"
    ).select(*column_names)
    deletes = manual_reviewed.filter(f.col("_change_type") == "delete").select(
        *column_names
    )
    logger.info(f"{inserts.count()=}, retain/{updates.count()=}, {deletes.count()=}")

    # Left join silver + updates to preserve full table
    # Potentially many NULLs on right side
    new_silver = silver.alias("silver").join(
        updates.alias("updates"), primary_key, "left"
    )

    # Choose right side values if not NULL, else left side
    new_silver = new_silver.withColumns(
        {
            f"resolved_{c}": f.coalesce(f.col(f"updates.{c}"), f.col(f"silver.{c}"))
            for c in column_names_no_pk
        }
    )

    # Select only the resolved columns
    new_silver = new_silver.select(
        *[primary_key, *[f"resolved_{c}" for c in column_names_no_pk]]
    )

    # Rename the resolved columns to the original column names
    new_silver = new_silver.withColumnsRenamed(
        {f"resolved_{c}": c for c in column_names_no_pk}
    )

    # Yeet the deletes
    new_silver = new_silver.join(deletes, primary_key, "left_anti")

    # Union the inserts; assume that column order is not consistent
    # Drop duplicate IDs just to be sure
    # Select expected columns just to be sure
    new_silver = (
        new_silver.unionByName(inserts)
        .dropDuplicates([primary_key])
        .select(*column_names)
    )

    return new_silver


def in_cluster_merge(
    master: sql.DataFrame,
    new: sql.DataFrame,
    primary_key: str,
    column_names: list[str],
) -> sql.DataFrame:
    """
    :param master: The full master table.
    :param new: The silver table whose schema already conforms to master.
    """
    column_names_no_pk = [*column_names]
    if primary_key in column_names_no_pk:
        column_names_no_pk.remove(primary_key)

    inserts = new.join(master, primary_key, "left_anti")
    updates = master.join(new.alias("new"), primary_key, "inner").select("new.*")
    deletes = master.join(new, primary_key, "left_anti")

    # Updates - inner join master + silver
    new_master = master.alias("master").join(
        updates.alias("updates"), primary_key, "inner"
    )

    # Get values from right side if not NULL, else left side
    new_master = new_master.withColumns(
        {
            f"resolved_{c}": f.coalesce(f.col(f"updates.{c}"), f.col(f"master.{c}"))
            for c in column_names_no_pk
        }
    )

    # Select only the resolved columns
    new_master = new_master.select(
        *[primary_key, *[f"resolved_{c}" for c in column_names_no_pk]]
    )

    # Rename the resolved columns to the original column names
    new_master = new_master.withColumnsRenamed(
        {f"resolved_{c}": c for c in column_names_no_pk}
    )

    # Yeet the deletes
    new_master = new_master.join(deletes, primary_key, "left_anti")

    # Union the inserts; assume that column order is not consistent
    # Drop duplicate IDs just to be sure
    # Select expected columns just to be sure
    new_master = (
        new_master.unionByName(inserts)
        .dropDuplicates([primary_key])
        .select(*column_names)
    )

    return new_master
