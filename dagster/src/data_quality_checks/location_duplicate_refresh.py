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
from pyspark.sql.types import DataType, StringType, StructField, StructType

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
from src.utils.schema import get_schema_columns_by_name, render_flag

# Same metaschema table dq_split_passed_rows reads for its dq_results rescue.
GEOLOCATION_SCHEMA_NAME = "school_geolocation"

PROXIMITY_COLUMNS = [
    "dq_duplicate_group_flag_50m",
    "dq_duplicate_group_count_50m",
    "dq_duplicate_group_id_50m",
]

EXACT_LOCATION_COLUMNS = [
    "dq_duplicate_location_rows_flag",
    "dq_duplicate_location_rows_count",
    "dq_duplicate_location_rows_id",
]

# A row already in a group, or with a neighbour, is one whose merge can shift a
# count for a school outside the upload.
GROUPED_ROW_COLUMNS = (
    "dq_duplicate_location_rows_count",
    "dq_duplicate_group_count_50m",
)


def _registered_types(
    schema_columns_by_name: dict[str, StructField], columns: list[str]
) -> dict[str, DataType]:
    """Registered metaschema type for each of ``columns``.

    Unlike dq_split_passed_rows, which just skips a column that isn't registered,
    these columns are this function's whole output — guessing a type for one that
    isn't registered would silently reintroduce the exact drift this refresh exists
    to fix. Fail loudly instead.
    """
    missing = [c for c in columns if c not in schema_columns_by_name]
    if missing:
        raise ValueError(
            f"Columns not registered in {GEOLOCATION_SCHEMA_NAME} metaschema: {missing}"
        )
    return {column: schema_columns_by_name[column].dataType for column in columns}


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
    schema_columns_by_name = get_schema_columns_by_name(
        df.sparkSession, GEOLOCATION_SCHEMA_NAME
    )

    exact_dtypes = _registered_types(schema_columns_by_name, EXACT_LOCATION_COLUMNS)

    count = f.count("*").over(Window.partitionBy(location_id_column()))
    exact_columns = location_duplicate_columns(count, null_coordinates(df))
    exact_columns["dq_duplicate_location_rows_flag"] = render_flag(
        exact_columns["dq_duplicate_location_rows_flag"],
        exact_dtypes["dq_duplicate_location_rows_flag"],
    )
    exact_columns["dq_duplicate_location_rows_count"] = exact_columns[
        "dq_duplicate_location_rows_count"
    ].cast(exact_dtypes["dq_duplicate_location_rows_count"])
    exact_columns["dq_duplicate_location_rows_id"] = exact_columns[
        "dq_duplicate_location_rows_id"
    ].cast(exact_dtypes["dq_duplicate_location_rows_id"])
    df = df.withColumns(exact_columns)

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

    # Name-keyed, not positional — assign_proximity_groups returns flag/group_id/
    # count in that order, which does not match PROXIMITY_COLUMNS' order.
    groups = groups.rename(
        columns={
            "flag": "dq_duplicate_group_flag_50m",
            "group_id": "dq_duplicate_group_id_50m",
            "count": "dq_duplicate_group_count_50m",
        }
    )
    groups["school_id_giga"] = groups["school_id_giga"].astype(str)

    group_dtypes = _registered_types(schema_columns_by_name, PROXIMITY_COLUMNS)
    if isinstance(group_dtypes["dq_duplicate_group_flag_50m"], StringType):
        groups["dq_duplicate_group_flag_50m"] = groups[
            "dq_duplicate_group_flag_50m"
        ].map({1: "Yes", 0: "No"})

    string_columns = [
        column
        for column in PROXIMITY_COLUMNS
        if isinstance(group_dtypes[column], StringType)
    ]
    int_columns = [c for c in PROXIMITY_COLUMNS if c not in string_columns]

    proximity_schema = StructType(
        [StructField("school_id_giga", StringType(), True)]
        + [
            StructField(column, group_dtypes[column], True)
            for column in PROXIMITY_COLUMNS
        ]
    )

    # Schools outside the graph keep NULL rather than 0 — "not evaluated" has to
    # stay distinguishable from "evaluated, no neighbour".
    return join_pandas_result_to_spark(
        df.drop(*PROXIMITY_COLUMNS),
        to_spark_safe(groups, int_columns, string_columns),
        PROXIMITY_COLUMNS,
        schema=proximity_schema,
    )
