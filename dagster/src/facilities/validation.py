from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.facilities.specs import FACILITY_ID_COLUMN, FacilityGeolocationSpec

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FacilityIntegrityError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class IntegrityIssue:
    code: str
    count: int


@dataclass(frozen=True, slots=True)
class FacilityReconciliation:
    core_count: int
    extension_count: int
    issues: tuple[IntegrityIssue, ...]

    @property
    def is_valid(self) -> bool:
        return not self.issues

    def raise_for_issues(self) -> None:
        if self.is_valid:
            return
        summary = ", ".join(f"{issue.code}={issue.count}" for issue in self.issues)
        raise FacilityIntegrityError(f"Facility reconciliation failed: {summary}")


def validate_owned_columns(
    dataframe: "DataFrame",
    spec: FacilityGeolocationSpec,
    *,
    require_extension: bool = True,
) -> None:
    required = set(spec.columns.core)
    if require_extension:
        required.update(spec.columns.extension)
    missing = sorted(required.difference(dataframe.columns))
    if missing:
        raise FacilityIntegrityError(
            f"Combined approved data is missing owned columns: {missing}"
        )


def facility_integrity_violations(
    core: "DataFrame",
    extension: "DataFrame",
    spec: FacilityGeolocationSpec,
) -> "DataFrame":
    """Return one row per violating ID and invariant, without collecting data."""
    from pyspark.sql import functions as f

    required_core = {FACILITY_ID_COLUMN, "facility_type", "core_signature"}
    required_extension = {FACILITY_ID_COLUMN, "extension_signature"}
    missing_core = required_core.difference(core.columns)
    missing_extension = required_extension.difference(extension.columns)
    if missing_core or missing_extension:
        raise FacilityIntegrityError(
            "Projected frames are missing integrity columns: "
            f"core={sorted(missing_core)}, extension={sorted(missing_extension)}"
        )

    core_ids = core.select(FACILITY_ID_COLUMN, "facility_type")
    extension_ids = extension.select(FACILITY_ID_COLUMN)

    null_core = core_ids.where(f.col(FACILITY_ID_COLUMN).isNull()).select(
        f.lit("null_core_id").alias("issue"),
        f.col(FACILITY_ID_COLUMN),
    )
    null_extension = extension_ids.where(
        f.col(FACILITY_ID_COLUMN).isNull()
    ).select(
        f.lit("null_extension_id").alias("issue"),
        f.col(FACILITY_ID_COLUMN),
    )
    duplicate_core = (
        core_ids.where(f.col(FACILITY_ID_COLUMN).isNotNull())
        .groupBy(FACILITY_ID_COLUMN)
        .count()
        .where(f.col("count") > 1)
        .select(
            f.lit("duplicate_core_id").alias("issue"),
            f.col(FACILITY_ID_COLUMN),
        )
    )
    duplicate_extension = (
        extension_ids.where(f.col(FACILITY_ID_COLUMN).isNotNull())
        .groupBy(FACILITY_ID_COLUMN)
        .count()
        .where(f.col("count") > 1)
        .select(
            f.lit("duplicate_extension_id").alias("issue"),
            f.col(FACILITY_ID_COLUMN),
        )
    )

    typed_core_ids = core_ids.where(
        f.col("facility_type") == f.lit(spec.facility_type)
    ).select(FACILITY_ID_COLUMN)
    missing_extension = typed_core_ids.join(
        extension_ids,
        on=FACILITY_ID_COLUMN,
        how="left_anti",
    ).select(
        f.lit("missing_extension").alias("issue"),
        f.col(FACILITY_ID_COLUMN),
    )
    orphan_extension = extension_ids.join(
        typed_core_ids,
        on=FACILITY_ID_COLUMN,
        how="left_anti",
    ).select(
        f.lit("orphan_extension").alias("issue"),
        f.col(FACILITY_ID_COLUMN),
    )
    type_mismatch = core_ids.where(
        f.col("facility_type") != f.lit(spec.facility_type)
    ).select(
        f.lit("facility_type_mismatch").alias("issue"),
        f.col(FACILITY_ID_COLUMN),
    )
    core_signature_values = [
        f.coalesce(f.col(column).cast("string"), f.lit("<null>"))
        for column in sorted(set(core.columns) - {"core_signature"})
    ]
    extension_signature_values = [
        f.coalesce(f.col(column).cast("string"), f.lit("<null>"))
        for column in sorted(set(extension.columns) - {"extension_signature"})
    ]
    core_signature_mismatch = core.where(
        f.col("core_signature").isNull()
        | (
            f.col("core_signature")
            != f.sha2(f.concat_ws("\x1f", *core_signature_values), 256)
        )
    ).select(
        f.lit("core_signature_mismatch").alias("issue"),
        f.col(FACILITY_ID_COLUMN),
    )
    extension_signature_mismatch = extension.where(
        f.col("extension_signature").isNull()
        | (
            f.col("extension_signature")
            != f.sha2(f.concat_ws("\x1f", *extension_signature_values), 256)
        )
    ).select(
        f.lit("extension_signature_mismatch").alias("issue"),
        f.col(FACILITY_ID_COLUMN),
    )

    violations = null_core
    for frame in (
        null_extension,
        duplicate_core,
        duplicate_extension,
        missing_extension,
        orphan_extension,
        type_mismatch,
        core_signature_mismatch,
        extension_signature_mismatch,
    ):
        violations = violations.unionByName(frame)
    return violations


def reconcile_facility_frames(
    core: "DataFrame",
    extension: "DataFrame",
    spec: FacilityGeolocationSpec,
) -> FacilityReconciliation:
    violations = facility_integrity_violations(core, extension, spec)
    issue_rows = violations.groupBy("issue").count().collect()
    issues = tuple(
        IntegrityIssue(code=row["issue"], count=row["count"])
        for row in sorted(issue_rows, key=lambda row: row["issue"])
    )
    return FacilityReconciliation(
        core_count=core.count(),
        extension_count=extension.count(),
        issues=issues,
    )


def validate_facility_frames(
    core: "DataFrame",
    extension: "DataFrame",
    spec: FacilityGeolocationSpec,
) -> FacilityReconciliation:
    reconciliation = reconcile_facility_frames(core, extension, spec)
    reconciliation.raise_for_issues()
    return reconciliation
