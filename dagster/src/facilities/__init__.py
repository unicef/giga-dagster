"""Facility package exports.

Heavy submodules (publish / projection / validation) are loaded lazily so Spark
workers can import ``src.facilities.identity`` for the stable-ID UDF without
pulling Dagster/settings dependencies that are not installed on workers.
"""

from src.facilities.identity import (
    FACILITY_ID_NAMESPACE,
    STABLE_FACILITY_ID_CONTRACT,
    StableFacilityIdContract,
    generate_facility_id,
)
from src.facilities.specs import (
    FACILITY_ID_COLUMN,
    HEALTH_FACILITY_SPEC,
    HEALTH_SPEC,
    SCHOOL_FACILITY_SPEC,
    SCHOOL_SPEC,
    FacilityAssetNames,
    FacilityColumnOwnership,
    FacilityGeolocationSpec,
    FacilityJobNames,
    FacilityPathNames,
    FacilityTableNames,
)

__all__ = [
    "FACILITY_ID_COLUMN",
    "FACILITY_ID_NAMESPACE",
    "HEALTH_FACILITY_SPEC",
    "HEALTH_SPEC",
    "SCHOOL_FACILITY_SPEC",
    "SCHOOL_SPEC",
    "STABLE_FACILITY_ID_CONTRACT",
    "FacilityAssetNames",
    "FacilityColumnOwnership",
    "FacilityGeolocationSpec",
    "FacilityIntegrityError",
    "FacilityJobNames",
    "FacilityPathNames",
    "FacilityProjections",
    "FacilityPublishResult",
    "FacilityReconciliation",
    "FacilityTableNames",
    "IntegrityIssue",
    "StableFacilityIdContract",
    "facility_integrity_violations",
    "generate_facility_id",
    "merge_approved_facilities",
    "project_facility_core",
    "project_facility_extension",
    "reconcile_facility_frames",
    "refresh_facility_signature",
    "split_approved_facilities",
    "validate_facility_frames",
    "validate_owned_columns",
]

_LAZY_EXPORTS = {
    "FacilityIntegrityError": ("src.facilities.validation", "FacilityIntegrityError"),
    "FacilityProjections": ("src.facilities.projection", "FacilityProjections"),
    "FacilityPublishResult": ("src.facilities.publish", "FacilityPublishResult"),
    "FacilityReconciliation": ("src.facilities.validation", "FacilityReconciliation"),
    "IntegrityIssue": ("src.facilities.validation", "IntegrityIssue"),
    "facility_integrity_violations": (
        "src.facilities.validation",
        "facility_integrity_violations",
    ),
    "merge_approved_facilities": (
        "src.facilities.publish",
        "merge_approved_facilities",
    ),
    "project_facility_core": ("src.facilities.projection", "project_facility_core"),
    "project_facility_extension": (
        "src.facilities.projection",
        "project_facility_extension",
    ),
    "reconcile_facility_frames": (
        "src.facilities.validation",
        "reconcile_facility_frames",
    ),
    "refresh_facility_signature": (
        "src.facilities.projection",
        "refresh_facility_signature",
    ),
    "split_approved_facilities": (
        "src.facilities.projection",
        "split_approved_facilities",
    ),
    "validate_facility_frames": (
        "src.facilities.validation",
        "validate_facility_frames",
    ),
    "validate_owned_columns": ("src.facilities.validation", "validate_owned_columns"),
}


def __getattr__(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target
    from importlib import import_module

    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value
