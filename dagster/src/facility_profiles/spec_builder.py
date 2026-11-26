"""Build FacilityGeolocationSpec from an ADLS profile + schema registry columns."""

from __future__ import annotations

from models.schema_registry import SchemaVersionColumn

from src.facilities.specs import (
    FacilityAssetNames,
    FacilityColumnOwnership,
    FacilityGeolocationSpec,
    FacilityJobNames,
    FacilityPathNames,
    FacilityTableNames,
)
from src.facility_profiles.schema import FacilityProfile
from src.utils.db.primary import get_db_context
from src.utils.schema_resolver import PostgresSchemaRegistry, resolve_schema_columns


def derive_asset_names(schema_key: str) -> FacilityAssetNames:
    """Shared pipeline logic; asset keys are unique per schema_key."""
    prefix = f"facility_geolocation_{schema_key}"
    return FacilityAssetNames(
        raw=f"{prefix}_raw",
        bronze=f"{prefix}_bronze",
        dq_results=f"{prefix}_data_quality_results",
        staging=f"{prefix}_staging",
        approved_rows=f"{prefix}_manual_review_passed_rows",
        silver=f"{prefix}_silver",
        core=f"{prefix}_core",
        extension=f"{prefix}_details",
        rejected_rows=f"{prefix}_manual_review_failed_rows",
    )


def derive_job_names(schema_key: str) -> FacilityJobNames:
    prefix = f"facility_geolocation_{schema_key}"
    return FacilityJobNames(
        automated_checks=f"{prefix}__automated_data_checks_job",
        post_approval=f"{prefix}__post_manual_checks_job",
        delete=f"{prefix}__admin_delete_rows_job",
    )


def _parse_domain_values(raw: str | None) -> tuple[object, ...] | None:
    if not raw:
        return None
    values = tuple(part.strip() for part in raw.split("|") if part.strip())
    return values or None


def column_checks_from_registry(
    columns: list[SchemaVersionColumn],
) -> tuple[
    tuple[tuple[str, tuple[object, ...]], ...],
    tuple[tuple[str, float, float], ...],
]:
    domains: list[tuple[str, tuple[object, ...]]] = []
    ranges: list[tuple[str, float, float]] = []
    for column in columns:
        domain = _parse_domain_values(column.domain_values)
        if domain is not None:
            domains.append((column.name, domain))
        if column.range_min is not None and column.range_max is not None:
            ranges.append(
                (column.name, float(column.range_min), float(column.range_max))
            )
    return tuple(domains), tuple(ranges)


def _fixture_columns_for_tests(
    schema_key: str,
) -> tuple[
    FacilityColumnOwnership,
    tuple[tuple[str, tuple[object, ...]], ...] | None,
    tuple[tuple[str, float, float], ...] | None,
]:
    """Column ownership used only when FACILITY_PROFILES_LOCAL_DIR is set (unit tests)."""
    from src.facilities.specs import (
        COMMON_CORE_COLUMNS,
        HEALTH_EXTENSION_COLUMNS,
        SCHOOL_EXTENSION_COLUMNS,
    )

    if schema_key == "schools":
        ownership = FacilityColumnOwnership(
            core=COMMON_CORE_COLUMNS,
            extension=SCHOOL_EXTENSION_COLUMNS,
        )
    elif schema_key == "health_facilities":
        ownership = FacilityColumnOwnership(
            core=COMMON_CORE_COLUMNS,
            extension=HEALTH_EXTENSION_COLUMNS,
        )
    else:
        raise ValueError(f"No test fixture columns for schema_key '{schema_key}'")
    return ownership, None, None


def load_columns_from_registry(
    schema_key: str,
) -> tuple[
    FacilityColumnOwnership,
    tuple[tuple[str, tuple[object, ...]], ...] | None,
    tuple[tuple[str, float, float], ...] | None,
]:
    from src.settings import settings

    # Unit tests only: avoid requiring Postgres when loading fixture YAMLs.
    if settings.FACILITY_PROFILES_LOCAL_DIR:
        return _fixture_columns_for_tests(schema_key)

    with get_db_context() as db:
        registry = PostgresSchemaRegistry(db)
        dataset = registry.get_dataset(schema_key)
        if dataset.role != "member":
            raise ValueError(
                f"Facility schema_key '{schema_key}' must be a member schema"
            )
        if dataset.group_id is None:
            raise ValueError(f"Facility schema_key '{schema_key}' has no group")
        core = registry.get_group_core(dataset.group_id)
        if core is None:
            raise ValueError(
                f"Facility schema group for '{schema_key}' has no core schema"
            )
        core_columns = registry.get_columns(registry.get_version(core, None))
        member_columns = registry.get_columns(registry.get_version(dataset, None))
        # Also available via resolve helper for pin consistency checks.
        _ = resolve_schema_columns(registry, schema_key)

    ownership = FacilityColumnOwnership(
        core=tuple(column.name for column in core_columns),
        extension=tuple(column.name for column in member_columns),
    )
    domains, ranges = column_checks_from_registry([*core_columns, *member_columns])
    return ownership, domains or None, ranges or None


def build_spec_from_profile(
    profile: FacilityProfile,
    *,
    columns: FacilityColumnOwnership,
    dq_domains: tuple[tuple[str, tuple[object, ...]], ...] | None = None,
    dq_ranges: tuple[tuple[str, float, float], ...] | None = None,
) -> FacilityGeolocationSpec:
    schema_key = profile.schema_key
    return FacilityGeolocationSpec(
        facility_type=profile.facility_type,
        domain=profile.dagster.domain,
        dataset_type=profile.dataset_type,
        govt_id_column=profile.identity.govt_id_column,
        giga_id_column=profile.identity.giga_id_column,
        name_column=profile.identity.name_column,
        level_column=profile.identity.level_column,
        country_column=profile.identity.country_column,
        columns=columns,
        tables=FacilityTableNames(
            core_schema=profile.tables.core_schema,
            core_table=profile.tables.core_table,
            extension_schema=profile.tables.extension_schema,
            extension_table=profile.tables.extension_table,
            staging_table=profile.tables.staging_table,
            silver_schema=profile.tables.silver_schema,
            rejected_schema=profile.tables.rejected_schema,
            error_table=profile.tables.error_table,
        ),
        paths=FacilityPathNames(
            upload_directory=profile.paths.upload_directory,
            dq_directory=profile.paths.dq_directory,
            approval_directory=profile.paths.approval_directory,
            delete_directory=profile.paths.delete_directory,
            schema_keys=(schema_key,),
        ),
        jobs=derive_job_names(schema_key),
        assets=derive_asset_names(schema_key),
        dq_config_table=profile.dagster.dq_config_table,
        approval_label=profile.portal.approval_label,
        group_name=profile.dagster.group_name,
        metastore_schema_name=profile.dagster.metastore_schema_name,
        enable_connectivity=profile.dagster.enable_connectivity,
        enable_registration_updates=profile.dagster.enable_registration_updates,
        dq_domains=dq_domains,
        dq_ranges=dq_ranges,
    )


def build_spec_for_schema_key(schema_key: str) -> FacilityGeolocationSpec:
    from src.facility_profiles.loader import get_facility_profile

    profile = get_facility_profile(schema_key)
    columns, domains, ranges = load_columns_from_registry(schema_key)
    return build_spec_from_profile(
        profile,
        columns=columns,
        dq_domains=domains,
        dq_ranges=ranges,
    )


def load_all_facility_specs() -> dict[str, FacilityGeolocationSpec]:
    from src.facility_profiles.loader import get_facility_profiles

    return {
        schema_key: build_spec_for_schema_key(schema_key)
        for schema_key in get_facility_profiles()
    }
