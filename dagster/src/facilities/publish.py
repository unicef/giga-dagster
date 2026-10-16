"""Pure Spark merge logic for publishing approved facilities to two tables."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.constants import StagingChangeType
from src.facilities.projection import split_approved_facilities
from src.facilities.specs import FACILITY_ID_COLUMN, FacilityGeolocationSpec
from src.internal.merge import core_merge_logic

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True, slots=True)
class FacilityPublishResult:
    core: "DataFrame"
    extension: "DataFrame"
    insert_count: int
    update_count: int
    delete_count: int


def _align_columns(current: "DataFrame", incoming: "DataFrame") -> tuple["DataFrame", "DataFrame"]:
    from pyspark.sql import functions as f

    current_types = {field.name: field.dataType for field in current.schema.fields}
    incoming_types = {field.name: field.dataType for field in incoming.schema.fields}
    ordered_columns = list(dict.fromkeys([*current.columns, *incoming.columns]))

    for column in ordered_columns:
        if column not in current.columns:
            current = current.withColumn(
                column,
                f.lit(None).cast(incoming_types[column]),
            )
        if column not in incoming.columns:
            incoming = incoming.withColumn(
                column,
                f.lit(None).cast(current_types[column]),
            )
    return current.select(*ordered_columns), incoming.select(*ordered_columns)


def _merge_one_table(
    current: "DataFrame",
    inserts: "DataFrame",
    updates: "DataFrame",
    deletes: "DataFrame",
) -> "DataFrame":
    current, inserts = _align_columns(current, inserts)
    _, updates = _align_columns(current, updates)
    _, deletes = _align_columns(current, deletes)
    return core_merge_logic(
        current,
        inserts,
        updates,
        deletes,
        FACILITY_ID_COLUMN,
        current.columns,
        update_join_type="left",
    )


def merge_approved_facilities(
    current_core: "DataFrame",
    current_extension: "DataFrame",
    approved: "DataFrame",
    spec: FacilityGeolocationSpec,
) -> FacilityPublishResult:
    """Idempotently apply approved INSERT/UPDATE/DELETE rows to both projections."""
    from pyspark.sql import functions as f

    if "change_type" not in approved.columns:
        raise ValueError("Approved facility rows must contain `change_type`")

    inserts_combined = approved.filter(
        f.col("change_type") == StagingChangeType.INSERT
    )
    updates_combined = approved.filter(
        f.col("change_type") == StagingChangeType.UPDATE
    )
    deletes_combined = approved.filter(
        f.col("change_type") == StagingChangeType.DELETE
    )

    inserts = split_approved_facilities(inserts_combined, spec, allow_missing=True)
    updates = split_approved_facilities(updates_combined, spec, allow_missing=True)
    deletes = split_approved_facilities(deletes_combined, spec, allow_missing=True)

    return FacilityPublishResult(
        core=_merge_one_table(
            current_core,
            inserts.core,
            updates.core,
            deletes.core,
        ),
        extension=_merge_one_table(
            current_extension,
            inserts.extension,
            updates.extension,
            deletes.extension,
        ),
        insert_count=inserts_combined.count(),
        update_count=updates_combined.count(),
        delete_count=deletes_combined.count(),
    )
