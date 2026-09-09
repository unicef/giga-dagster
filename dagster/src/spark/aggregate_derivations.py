"""Derivation of aggregate columns from their components.

The rules live in `config_expectations` and are consumed here (to fill an
aggregate that was not supplied) and by `data_quality_checks.column_relation`
(to flag an aggregate that contradicts its components). Both sides share the
expression builders below so the two can never drift apart.
"""

from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, IntegerType, StringType

from dagster import OpExecutionContext
from src.data_quality_checks.config import (
    CONFIG_AGGREGATE_SUM,
    CONFIG_AVAILABILITY_FROM_COUNT,
)
from src.utils.logger import get_context_with_fallback_logger


def aggregate_relation_check_name(target: str, parts: list[str]) -> str:
    """Check column name for an aggregate rule, e.g.
    dq_column_relation_checks-num_students_num_students_girls_num_students_boys.

    Must match the "DQ Table Column Name" registered in the NocoDB
    SchoolGeolocationMasterDQChecks table, otherwise the check is computed but
    never surfaces in the DQ report.
    """
    return f"dq_column_relation_checks-{'_'.join([target, *parts])}"


def aggregate_rules(dataset_type: str) -> list[tuple[str, list[str], bool]]:
    """Configured aggregate rules as (target, components, is_availability).

    Single source for the derivation, the column relation check and the
    human-readable descriptions, so the three cannot fall out of step.
    """
    return [
        (target, parts, False)
        for target, parts in CONFIG_AGGREGATE_SUM.get(dataset_type, {}).items()
    ] + [
        (target, parts, True)
        for target, parts in CONFIG_AVAILABILITY_FROM_COUNT.get(
            dataset_type, {}
        ).items()
    ]


def present_parts(df: sql.DataFrame, parts: list[str]) -> list[str]:
    """Keep only the component columns that actually exist on the dataframe."""
    return [part for part in parts if part in df.columns]


def sum_of_parts(df: sql.DataFrame, parts: list[str]) -> sql.Column:
    """Null-safe sum of the component columns.

    Returns NULL when every present component is NULL, so that "no information"
    is never conflated with a genuine zero. Components are cast to INT because
    the bronze dataframe carries every uploaded value as a string.
    """
    available = present_parts(df, parts)
    if not available:
        return f.lit(None).cast(IntegerType())

    casted = [f.col(part).cast(IntegerType()) for part in available]
    any_not_null = f.lit(False)
    for part in casted:
        any_not_null = any_not_null | part.isNotNull()

    total = casted[0]
    for part in casted[1:]:
        total = f.coalesce(total, f.lit(0)) + f.coalesce(part, f.lit(0))

    return f.when(any_not_null, f.coalesce(total, f.lit(0))).otherwise(
        f.lit(None).cast(IntegerType())
    )


# Countries do not agree on how to spell a yes/no answer, so the comparison
# parses the supplied value rather than matching "yes"/"no" literally.
AVAILABILITY_TRUE_VALUES = ("yes", "y", "true", "1")
AVAILABILITY_FALSE_VALUES = ("no", "n", "false", "0")


def availability_as_boolean(column: sql.Column) -> sql.Column:
    """Parse a supplied availability value into a boolean.

    NULL for anything unrecognised, so that an odd value ("Unknown", a typo) is
    left to the domain checks instead of being reported as a contradiction.
    """
    normalized = f.lower(f.trim(column))
    return (
        f.when(normalized.isin(list(AVAILABILITY_TRUE_VALUES)), f.lit(True))
        .when(normalized.isin(list(AVAILABILITY_FALSE_VALUES)), f.lit(False))
        .otherwise(f.lit(None).cast(BooleanType()))
    )


def availability_from_count(count_col: sql.Column) -> sql.Column:
    """Map a counter to a Yes/No availability value, preserving NULL."""
    return (
        f.when(count_col.isNull(), f.lit(None).cast(StringType()))
        .when(count_col > 0, f.lit("Yes"))
        .otherwise(f.lit("No"))
    )


def _target_type(df: sql.DataFrame, target: str):
    return df.schema[target].dataType


def derive_aggregate_columns(
    df: sql.DataFrame,
    dataset_type: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Fill aggregate columns that were not supplied, from their components.

    Only NULL targets are written: a value provided by the country is always
    kept, even when it contradicts its components. That contradiction is
    reported by the matching column relation check instead.
    """
    logger = get_context_with_fallback_logger(context)

    column_actions = {}
    for target, parts, is_availability in aggregate_rules(dataset_type):
        # The target must already be part of the schema: staging drops any
        # column outside it, so deriving one would be silently discarded.
        if target not in df.columns:
            continue
        available = present_parts(df, parts)
        if not available:
            continue

        total = sum_of_parts(df, available)
        derived = availability_from_count(total) if is_availability else total

        column_actions[target] = f.when(
            f.col(target).isNull(), derived.cast(_target_type(df, target))
        ).otherwise(f.col(target))
        logger.info(f"Deriving {target} from {available} where null...")

    if not column_actions:
        return df

    return df.withColumns(column_actions)
