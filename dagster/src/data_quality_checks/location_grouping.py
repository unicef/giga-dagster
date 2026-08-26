"""Location-grouping primitives shared by the DQ checks and the silver merge.

Each function takes an optional ``reference`` frame of rows that participate in
the grouping but never appear in the output. ``None`` restores file-scoped
behaviour, which is what master wants since its input is already the full dataset.
"""

import hashlib

import networkx as nx
import pandas as pd
from networkx.algorithms.clique import find_cliques as maximal_cliques
from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.types import StructType

from src.utils.logger import get_context_with_fallback_logger

# Spark equi-joins drop null keys, whereas Window.partitionBy groups them
# together; the sentinel keeps the union-based grouping equivalent.
_NULL_SENTINEL = "\u0002"
_KEY_SEPARATOR = "\u0001"

GROUP_KEY_COLUMN = "_group_key"
GROUP_COUNT_COLUMN = "_group_count"

# Shared identity columns for every duplicate-group-member frame in the duplicates
# report; each check type appends its own group id/count columns to this prefix.
MEMBER_IDENTITY_SCHEMA = (
    "school_id_govt string, latitude double, longitude double, source string"
)
LOCATION_MEMBERS_SCHEMA = (
    f"{MEMBER_IDENTITY_SCHEMA}, duplicate_location_rows_id string, "
    "duplicate_location_rows_count int"
)


def null_coordinates(df: sql.DataFrame) -> sql.Column:
    return (
        f.col("latitude").isNull()
        | f.isnan(f.col("latitude"))
        | f.col("longitude").isNull()
        | f.isnan(f.col("longitude"))
    )


def location_id_column() -> sql.Column:
    """Key identifying an exact coordinate pair.

    Shared so the DQ run and the post-merge refresh hash the same string, and
    ``duplicate_location_rows_ID`` stays stable between them.
    """
    return f.concat_ws(
        "_",
        f.col("longitude").cast("string"),
        f.col("latitude").cast("string"),
    )


def location_duplicate_columns(
    count_col: sql.Column, null_coords: sql.Column
) -> dict[str, sql.Column]:
    """The three exact-location duplicate expressions, keyed by plain column name.

    Shared by the DQ run and the post-merge refresh so the two cannot drift — the
    ID has to hash identically on both sides.
    """
    return {
        "duplicate_location_rows_flag": f.when(null_coords, f.lit(None).cast("int"))
        .when(count_col > 1, 1)
        .otherwise(0),
        "duplicate_location_rows_count": f.when(
            null_coords, f.lit(None).cast("int")
        ).otherwise(count_col.cast("int")),
        "duplicate_location_rows_ID": f.when(null_coords, f.lit(None)).otherwise(
            f.substring(f.md5(location_id_column()), 1, 8)
        ),
    }


def to_spark_safe(
    pdf: pd.DataFrame, int_columns: list[str], string_columns: list[str] = ()
) -> pd.DataFrame:
    """Convert nullable pandas dtypes to object columns Spark can infer from."""
    for column in int_columns:
        pdf[column] = [None if pd.isna(v) else int(v) for v in pdf[column]]
    for column in string_columns:
        pdf[column] = [None if pd.isna(v) else str(v) for v in pdf[column]]
    return pdf


def join_pandas_result_to_spark(
    df: sql.DataFrame,
    result_pdf: pd.DataFrame,
    result_columns: list[str],
    schema: StructType = None,
) -> sql.DataFrame:
    """Left-join a Pandas result onto ``df`` by school_id_giga."""
    result_keyed = result_pdf[["school_id_giga"] + result_columns].drop_duplicates(
        subset="school_id_giga", keep="first"
    )
    result_sdf = df.sparkSession.createDataFrame(result_keyed, schema=schema)
    return df.join(result_sdf, on="school_id_giga", how="left")


def group_key(columns: list[str]) -> sql.Column:
    return f.concat_ws(
        _KEY_SEPARATOR,
        *[
            f.coalesce(f.col(column).cast("string"), f.lit(_NULL_SENTINEL))
            for column in columns
        ],
    )


def build_reference_frame(
    silver: sql.DataFrame,
    upload: sql.DataFrame,
    columns: list[str],
    context=None,
) -> sql.DataFrame:
    """Silver rows that group alongside ``upload``.

    Silver rows whose ``school_id_govt`` is in the upload are excluded — the upload
    carries their new version, so keeping both makes every updated school a
    duplicate of itself.
    """
    logger = get_context_with_fallback_logger(context)

    if silver is None or "school_id_govt" not in silver.columns:
        return None

    missing = [column for column in columns if column not in silver.columns]
    if missing:
        logger.info(f"No location reference frame — silver is missing {missing}")
        return None

    upload_ids = upload.select("school_id_govt").distinct()
    return (
        silver.where(~null_coordinates(silver))
        .join(upload_ids, on="school_id_govt", how="left_anti")
        .select(*columns)
    )


def add_group_counts(
    df: sql.DataFrame,
    reference: sql.DataFrame,
    columns: list[str],
    count_column: str = None,
) -> sql.DataFrame:
    """Add ``_group_count``: rows sharing ``columns`` across ``df`` and ``reference``.

    ``count_column`` counts only rows where that column is non-null, matching
    ``count(col)`` rather than ``count(*)``.
    """
    df = df.withColumn(GROUP_KEY_COLUMN, group_key(columns))

    def keys_of(frame: sql.DataFrame) -> sql.DataFrame:
        if count_column is not None:
            frame = frame.where(f.col(count_column).isNotNull())
        return frame.select(group_key(columns).alias(GROUP_KEY_COLUMN))

    keys = df.select(GROUP_KEY_COLUMN) if count_column is None else keys_of(df)
    if reference is not None:
        keys = keys.unionByName(keys_of(reference))

    counts = keys.groupBy(GROUP_KEY_COLUMN).agg(
        f.count("*").cast("int").alias(GROUP_COUNT_COLUMN)
    )
    return (
        df.join(counts, on=GROUP_KEY_COLUMN, how="left")
        .withColumn(GROUP_COUNT_COLUMN, f.coalesce(f.col(GROUP_COUNT_COLUMN), f.lit(0)))
        .drop(GROUP_KEY_COLUMN)
    )


def materialize_location_duplicate_members(
    df: sql.DataFrame, reference: sql.DataFrame = None
) -> sql.DataFrame:
    """Every row (file or master) belonging to a flagged exact-location duplicate group.

    ``df`` must already carry ``dq_duplicate_location_rows_flag/_id/_count`` (i.e. this
    runs after ``duplicate_set_checks``).
    """
    if "dq_duplicate_location_rows_flag" not in df.columns:
        return df.sparkSession.createDataFrame([], LOCATION_MEMBERS_SCHEMA)

    flagged = df.filter(f.col("dq_duplicate_location_rows_flag") == 1).withColumn(
        "location_id", location_id_column()
    )
    file_members = flagged.select(
        "school_id_govt",
        "latitude",
        "longitude",
        f.lit("file").alias("source"),
        f.col("dq_duplicate_location_rows_id").alias("duplicate_location_rows_id"),
        f.col("dq_duplicate_location_rows_count").alias(
            "duplicate_location_rows_count"
        ),
    )

    if reference is None:
        return file_members

    # The group's count is shared by every member, so any flagged file row's count
    # broadcasts onto the master rows sharing its location_id.
    group_counts = flagged.select(
        "location_id", "dq_duplicate_location_rows_count"
    ).distinct()
    master_members = (
        reference.withColumn("location_id", location_id_column())
        .join(group_counts, on="location_id", how="inner")
        .select(
            "school_id_govt",
            "latitude",
            "longitude",
            f.lit("master").alias("source"),
            f.substring(f.md5(f.col("location_id")), 1, 8).alias(
                "duplicate_location_rows_id"
            ),
            f.col("dq_duplicate_location_rows_count").alias(
                "duplicate_location_rows_count"
            ),
        )
    )
    return file_members.unionByName(master_members)


def combine_duplicate_members(
    location_members: sql.DataFrame, fifty_m_members: sql.DataFrame
) -> sql.DataFrame:
    """One row per school (file or master) in either duplicate group."""
    a = location_members.select(
        "school_id_govt",
        f.col("latitude").alias("_a_lat"),
        f.col("longitude").alias("_a_lon"),
        f.col("source").alias("_a_source"),
        "duplicate_location_rows_id",
        "duplicate_location_rows_count",
    )
    b = fifty_m_members.select(
        "school_id_govt",
        f.col("latitude").alias("_b_lat"),
        f.col("longitude").alias("_b_lon"),
        f.col("source").alias("_b_source"),
        "duplicate_group_id_50m",
        "duplicate_group_count_50m",
    )
    return a.join(b, on="school_id_govt", how="full_outer").select(
        "school_id_govt",
        f.coalesce(f.col("_a_lat"), f.col("_b_lat")).alias("latitude"),
        f.coalesce(f.col("_a_lon"), f.col("_b_lon")).alias("longitude"),
        f.coalesce(f.col("_a_source"), f.col("_b_source")).alias("source"),
        "duplicate_location_rows_id",
        "duplicate_location_rows_count",
        "duplicate_group_id_50m",
        "duplicate_group_count_50m",
    )


def attach_approval_status(
    duplicates_report: sql.DataFrame, dq_results: sql.DataFrame
) -> sql.DataFrame:
    """approval_status is only meaningful for "file" rows — master rows have no DQ
    run of their own for this upload, so they stay NULL."""
    relevant_ids = duplicates_report.select("school_id_govt").distinct()
    approval = (
        dq_results.join(relevant_ids, on="school_id_govt", how="inner")
        .select("school_id_govt", "dq_has_critical_error")
        .dropDuplicates(["school_id_govt"])
        .withColumn(
            "approval_status",
            f.when(f.col("dq_has_critical_error") == 1, "rejected").otherwise(
                "approved"
            ),
        )
        .drop("dq_has_critical_error")
    )
    return duplicates_report.join(approval, on="school_id_govt", how="left")


def _partition_graph_by_max_cliques(graph: nx.Graph) -> list:
    """Partition a graph into maximal cliques for duplicate grouping."""

    def total_clique_weight(graph, clique):
        return sum(
            graph[u][v].get("weight", 1)
            for u in clique
            for v in clique
            if u < v and graph.has_edge(u, v)
        )

    remaining = graph.copy()
    cliques = []
    while len(remaining.nodes) > 0:
        all_cliques = list(maximal_cliques(remaining))
        max_size = max(len(c) for c in all_cliques)
        largest_cliques = [c for c in all_cliques if len(c) == max_size]
        chosen_clique = min(
            largest_cliques, key=lambda c: total_clique_weight(remaining, c)
        )
        cliques.append(chosen_clique)
        remaining.remove_nodes_from(chosen_clique)
    return cliques


def assign_proximity_groups(graph: nx.Graph) -> pd.DataFrame:
    """Resolve a proximity graph into flag / group id / neighbour count per node.

    Connected components are partitioned into maximal cliques so a chain of
    near-neighbours is not collapsed into one oversized group. The group id is an
    8-char md5 hash of its sorted members, matching duplicate_location_rows_ID's
    hash-based ID convention.
    """
    groups = []
    for component in nx.connected_components(graph):
        subgraph = graph.subgraph(component).copy()
        if len(subgraph) <= 1:
            continue
        groups.extend(
            sorted(clique)
            for clique in _partition_graph_by_max_cliques(subgraph)
            if len(clique) > 1
        )

    duplicate_map = {}
    for members in groups:
        group_id = hashlib.md5(
            ",".join(str(m) for m in members).encode(), usedforsecurity=False
        ).hexdigest()[:8]
        for node in members:
            duplicate_map[node] = group_id

    nodes = list(graph.nodes())
    return pd.DataFrame(
        {
            "school_id_giga": nodes,
            "flag": [1 if node in duplicate_map else 0 for node in nodes],
            "group_id": [duplicate_map.get(node) for node in nodes],
            # +1 so count includes the row itself, matching duplicate_location_rows_count.
            "count": [graph.degree(node) + 1 for node in nodes],
        }
    )
