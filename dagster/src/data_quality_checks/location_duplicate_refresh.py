"""Refresh location duplicate columns across the whole dataset after a merge.

The DQ run computes these columns for the uploaded rows, but a school joining or
leaving a duplicate group also changes the counts of the schools next to it, and
those are never part of the upload. Once staging has merged into silver the full
row set is known, so the columns are recomputed there for every school.
"""

from functools import reduce

from pyspark import sql
from pyspark.sql import (
    Window,
    functions as f,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.data_quality_checks.geospatial import (
    PROXIMITY_DUPLICATE_THRESHOLD_M,
    build_proximity_graph,
)
from src.data_quality_checks.location_grouping import (
    assign_proximity_groups,
    join_pandas_result_to_spark,
    location_duplicate_columns,
    location_id_column,
    null_coordinates,
    to_spark_safe,
)
from src.utils.logger import get_context_with_fallback_logger

PROXIMITY_INT_COLUMNS = ["duplicate_group_flag_50", "duplicate_group_count_50"]
PROXIMITY_STRING_COLUMNS = ["duplicate_group_id_50"]
PROXIMITY_COLUMNS = PROXIMITY_INT_COLUMNS + PROXIMITY_STRING_COLUMNS

# A row already in a group, or with a neighbour, is one whose merge can shift a
# count for a school outside the upload.
GROUPED_ROW_COLUMNS = ("duplicate_location_rows_count", "duplicate_group_count_50")

PROXIMITY_SCHEMA = StructType(
    [StructField("school_id_giga", StringType(), True)]
    + [StructField(column, IntegerType(), True) for column in PROXIMITY_INT_COLUMNS]
    + [StructField(column, StringType(), True) for column in PROXIMITY_STRING_COLUMNS]
)


def _is_grouped(frame: sql.DataFrame) -> sql.Column:
    """Rows that belong to, or touch, a duplicate group.

    Keyed off the counts rather than the flags: the clique partition drops
    singleton groups, so a school can sit within 50m of another (count 2) while
    its own flag stays 0 — and that neighbour's count still changed. Both counts
    include the row itself, so 1 is the unique baseline.
    """
    present = [c for c in GROUPED_ROW_COLUMNS if c in frame.columns]
    if not present:
        return None
    return reduce(lambda a, b: a | b, [f.col(c) > 1 for c in present])


def needs_refresh(
    current: sql.DataFrame,
    merged: sql.DataFrame,
    primary_key: str,
    changed_ids: list[str],
) -> bool:
    """Whether any approved row joins or leaves a duplicate group."""
    if not changed_ids:
        return False

    for frame in (merged, current):
        if frame is None:
            continue
        grouped = _is_grouped(frame)
        if grouped is None:
            continue
        touched = frame.where(f.col(primary_key).isin(changed_ids)).where(grouped)
        if not touched.limit(1).isEmpty():
            return True

    return False


def refresh_location_duplicates(
    df: sql.DataFrame,
    context=None,
) -> sql.DataFrame:
    """Recompute the exact-location and 50m duplicate columns over all of ``df``."""
    logger = get_context_with_fallback_logger(context)

    count = f.count("*").over(Window.partitionBy(location_id_column()))
    df = df.withColumns(location_duplicate_columns(count, null_coordinates(df)))

    points = df.select("school_id_giga", "latitude", "longitude").toPandas()
    graph = build_proximity_graph(
        points, PROXIMITY_DUPLICATE_THRESHOLD_M, context=context
    )
    if graph is None:
        return df

    logger.info(
        f"Proximity refresh: nodes={graph.number_of_nodes()}, "
        f"edges={graph.number_of_edges()}"
    )

    groups = assign_proximity_groups(graph)
    if groups.empty:
        return df

    groups = groups.rename(
        columns=dict(zip(["flag", "group_id", "count"], PROXIMITY_COLUMNS, strict=True))
    )
    groups["school_id_giga"] = groups["school_id_giga"].astype(str)

    # Schools outside the graph keep NULL rather than 0 — "not evaluated" has to
    # stay distinguishable from "evaluated, no neighbour".
    return join_pandas_result_to_spark(
        df.drop(*PROXIMITY_COLUMNS),
        to_spark_safe(groups, PROXIMITY_INT_COLUMNS, PROXIMITY_STRING_COLUMNS),
        PROXIMITY_COLUMNS,
        schema=PROXIMITY_SCHEMA,
    )
