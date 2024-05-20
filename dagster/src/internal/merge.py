from typing import Literal

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


def core_merge_logic(
    master: sql.DataFrame,
    inserts: sql.DataFrame,
    updates: sql.DataFrame,
    deletes: sql.DataFrame,
    primary_key: str,
    column_names: list[str],
    update_join_type: Literal["inner", "left"],
) -> sql.DataFrame:
    column_names_no_pk_sig = [*column_names]
    if primary_key in column_names_no_pk_sig:
        column_names_no_pk_sig.remove(primary_key)
    if "signature" in column_names_no_pk_sig:
        column_names_no_pk_sig.remove("signature")

    # Updates - inner join master + silver
    new_master = master.alias("master").join(
        updates.alias("updates"), primary_key, update_join_type
    )

    # Get values from right side if not NULL, else left side
    new_master = new_master.withColumns(
        {
            f"resolved_{c}": f.coalesce(f.col(f"updates.{c}"), f.col(f"master.{c}"))
            for c in column_names_no_pk_sig
        }
    )

    # Select only the resolved columns
    new_master = new_master.select(
        *[primary_key, *[f"resolved_{c}" for c in column_names_no_pk_sig]]
    )

    # Rename the resolved columns to the original column names
    new_master = new_master.withColumnsRenamed(
        {f"resolved_{c}": c for c in column_names_no_pk_sig}
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


def partial_cdf_in_cluster_merge(
    master: sql.DataFrame,
    incoming: sql.DataFrame,
    column_names: list[str],
    primary_key: str,
    context: OpExecutionContext = None,
):
    logger = get_context_with_fallback_logger(context)

    column_names_no_pk_sig = [*column_names]
    if primary_key in column_names_no_pk_sig:
        column_names_no_pk_sig.remove(primary_key)
    if "signature" in column_names_no_pk_sig:
        column_names_no_pk_sig.remove("signature")

    # Extract operations; likely these will be partial tables.
    # Select only update_postimage since this is the only one we show in
    # the approve rows workflow.
    inserts = incoming.filter(f.col("_change_type") == "insert").select(*column_names)
    updates = incoming.filter(f.col("_change_type") == "update_postimage").select(
        *column_names
    )
    deletes = incoming.filter(f.col("_change_type") == "delete").select(*column_names)
    logger.info(f"{inserts.count()=}, retain/{updates.count()=}, {deletes.count()=}")

    new_silver = core_merge_logic(
        master,
        inserts,
        updates,
        deletes,
        primary_key,
        column_names,
        update_join_type="left",
    )

    return new_silver


def partial_in_cluster_merge(
    master: sql.DataFrame,
    new: sql.DataFrame,
    primary_key: str,
    column_names: list[str],
) -> sql.DataFrame:
    """
    Inserts and deletes become ambiguous in a partial merge, so deletes will not be handled here.
    """
    column_names_no_pk_sig = [*column_names]
    if primary_key in column_names_no_pk_sig:
        column_names_no_pk_sig.remove(primary_key)
    if "signature" in column_names_no_pk_sig:
        column_names_no_pk_sig.remove("signature")

    inserts = new.join(master, primary_key, "left_anti")
    updates = master.join(new.alias("new"), primary_key, "inner").select("new.*")
    deletes = master.sparkSession.createDataFrame([], master.schema)

    new_master = core_merge_logic(
        master,
        inserts,
        updates,
        deletes,
        primary_key,
        column_names,
        update_join_type="left",
    )

    return new_master


def full_in_cluster_merge(
    master: sql.DataFrame,
    new: sql.DataFrame,
    primary_key: str,
    column_names: list[str],
) -> sql.DataFrame:
    """
    :param master: The full master table. Note that this doesn't actually have to be
                   school master. It just needs to be the current version of a Delta Table.
    :param new: The full table of incoming changes whose schema already conforms to `master`.
    """
    column_names_no_pk_sig = [*column_names]
    if primary_key in column_names_no_pk_sig:
        column_names_no_pk_sig.remove(primary_key)
    if "signature" in column_names_no_pk_sig:
        column_names_no_pk_sig.remove("signature")

    inserts = new.join(master, primary_key, "left_anti")
    updates = master.join(new.alias("new"), primary_key, "inner").select("new.*")
    deletes = master.join(new, primary_key, "left_anti")

    new_master = core_merge_logic(
        master,
        inserts,
        updates,
        deletes,
        primary_key,
        column_names,
        update_join_type="inner",
    )

    return new_master
