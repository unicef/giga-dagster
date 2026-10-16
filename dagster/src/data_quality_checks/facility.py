"""Adapters that run the mature geolocation checks against any facility profile.

The current check implementations use the historical school column names internally.
This module provides those names as temporary aliases and removes them afterwards,
allowing the check algorithms to be reused while their public input remains facility
specific.
"""

from dagster import OpExecutionContext
from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from src.data_quality_checks.create_update import (
    add_is_new_entity,
    enrich_with_current_values,
)
from src.data_quality_checks.dq_context import DQContext, DQMode
from src.data_quality_checks.utils import row_level_checks
from src.facilities import STABLE_FACILITY_ID_CONTRACT, FacilityGeolocationSpec
from src.spark.transform_functions import add_admin_columns, add_disputed_region_column
from src.utils.spark import normalize_missing_value_strings

_CHECK_ALIASES = {
    "govt_id_column": "school_id_govt",
    "giga_id_column": "school_id_giga",
    "name_column": "school_name",
    "level_column": "education_level",
}


def _column_replacements(
    spec: FacilityGeolocationSpec,
) -> tuple[tuple[str, str], ...]:
    return (
        ("education_level_govt", spec.level_column),
        ("school_id_govt", spec.govt_id_column),
        ("school_id_giga", spec.giga_id_column),
        ("school_name", spec.name_column),
        ("education_level", spec.level_column),
        ("school_density", "facility_density"),
        ("school_code", "facility_code"),
    )


def _dq_column_aliases(spec: FacilityGeolocationSpec) -> dict[str, str]:
    internal_columns = (
        "dq_duplicate-school_id_govt",
        "dq_duplicate-school_id_giga",
        "dq_is_null_mandatory-school_id_govt",
        "dq_is_null_optional-school_name",
        "dq_is_null_optional-education_level",
        "dq_is_null_optional-education_level_govt",
        "dq_is_not_alphanumeric-school_name",
        "dq_is_not_alphanumeric-education_level",
        (
            "dq_duplicate_set-school_id_govt_school_name_"
            "education_level_location_id"
        ),
        "dq_duplicate_set-school_name_education_level_location_id",
        "dq_duplicate_set-education_level_location_id",
        "dq_duplicate_all_except_school_code",
        "dq_is_school_density_greater_than_5",
        "dq_school_density_count",
        "dq_school_density_group_id",
    )
    aliases = {}
    for internal in internal_columns:
        external = internal
        for school_name, facility_name in _column_replacements(spec):
            external = external.replace(school_name, facility_name)
        if external != internal:
            aliases[external] = internal
    return aliases


def _rename_facility_dq_columns(
    df: sql.DataFrame,
    spec: FacilityGeolocationSpec,
    aliases: dict[str, str],
) -> sql.DataFrame:
    all_aliases = dict(aliases)
    for internal in df.columns:
        if not internal.startswith("dq_"):
            continue
        external = internal
        for school_name, facility_name in _column_replacements(spec):
            external = external.replace(school_name, facility_name)
        if external != internal:
            all_aliases.setdefault(external, internal)
    for external, internal in all_aliases.items():
        if internal not in df.columns:
            continue
        if external in df.columns:
            df = df.drop(internal)
        else:
            df = df.withColumnRenamed(internal, external)
    return df


def add_check_compatibility_columns(
    df: sql.DataFrame,
    spec: FacilityGeolocationSpec,
) -> tuple[sql.DataFrame, tuple[str, ...]]:
    """Add temporary historical aliases required by the geolocation check engine."""
    added = []
    for spec_attribute, check_column in _CHECK_ALIASES.items():
        source_column = getattr(spec, spec_attribute)
        if source_column == check_column:
            continue
        if source_column not in df.columns:
            raise ValueError(
                f"Facility data is missing `{source_column}` required for `{check_column}`"
            )
        if check_column not in df.columns:
            df = df.withColumn(check_column, f.col(source_column))
            added.append(check_column)
    if "education_level_govt" not in df.columns:
        df = df.withColumn("education_level_govt", f.col(spec.level_column))
        added.append("education_level_govt")
    return df, tuple(added)


def prepare_facility_bronze(
    df: sql.DataFrame,
    current: sql.DataFrame,
    spec: FacilityGeolocationSpec,
    country_code: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Apply common identity and boundary enrichment before facility DQ."""
    df = normalize_missing_value_strings(df)
    if spec.country_column not in df.columns:
        df = df.withColumn(spec.country_column, f.lit(country_code))

    current_ids = (
        current
        if spec.govt_id_column in current.columns
        else current.sparkSession.createDataFrame(
            [],
            f"{spec.govt_id_column} string, {spec.giga_id_column} string",
        )
    )
    df = add_is_new_entity(
        df,
        current_ids,
        govt_id_column=spec.govt_id_column,
        output_column="is_new_facility",
        context=context,
    )
    df = STABLE_FACILITY_ID_CONTRACT.add_to_dataframe(
        df,
        output_column=spec.giga_id_column,
        facility_type=spec.facility_type,
        country_column=spec.country_column,
        govt_id_column=spec.govt_id_column,
    )

    if {"latitude", "longitude"}.issubset(df.columns):
        for admin_level in ("admin1", "admin2", "admin3", "admin4"):
            df = add_admin_columns(df, country_code, admin_level)
        df = add_disputed_region_column(df)
    return df


def run_facility_geolocation_checks(
    bronze: sql.DataFrame,
    current: sql.DataFrame,
    spec: FacilityGeolocationSpec,
    country_code: str,
    context: OpExecutionContext = None,
    extra_outputs: dict | None = None,
) -> sql.DataFrame:
    """Run shared geolocation checks while retaining facility-specific columns."""
    enriched = enrich_with_current_values(
        bronze,
        current,
        govt_id_column=spec.govt_id_column,
        giga_id_column=spec.giga_id_column,
        is_new_column="is_new_facility",
        context=context,
    )
    compatible, aliases = add_check_compatibility_columns(enriched, spec)
    compatible_current, _ = add_check_compatibility_columns(current, spec)
    dq_aliases = _dq_column_aliases(spec)
    checked = row_level_checks(
        compatible,
        silver=compatible_current,
        dq_context=DQContext(
            dq_mode=DQMode.MASTER,
            dataset_type="geolocation",
            country_code_iso3=country_code,
            dq_config_table=spec.dq_config_table,
            is_new_column="is_new_facility",
            check_column_aliases=tuple(dq_aliases.items()),
            values_domain=spec.dq_domains,
            values_range=spec.dq_ranges,
            duplicate_all_except_columns=tuple(
                column
                for column in spec.columns.all
                if column not in {spec.govt_id_column, spec.giga_id_column}
            ),
            unique_set_columns=(
                (
                    "school_id_govt",
                    "school_name",
                    "education_level",
                    "location_id",
                ),
                ("school_name", "education_level", "location_id"),
                ("education_level", "location_id"),
                ("location_id",),
            ),
        ),
        context=context,
        extra_outputs=extra_outputs,
    )
    return _rename_facility_dq_columns(checked, spec, dq_aliases).drop(*aliases)


def split_facility_dq_rows(
    df: sql.DataFrame,
) -> tuple[sql.DataFrame, sql.DataFrame]:
    """Return passing and failing facility rows without applying a school schema."""
    passed = df.filter(f.col("dq_has_critical_error") == 0)
    failed = df.filter(f.col("dq_has_critical_error") == 1)
    return passed, failed


def collapse_facility_dq_flags(df: sql.DataFrame) -> sql.DataFrame:
    """Store integer DQ flags in the standard map while retaining string group IDs."""
    flag_columns = [
        field.name
        for field in df.schema.fields
        if field.name.startswith("dq_")
        and field.name != "dq_has_critical_error"
        and not isinstance(field.dataType, StringType)
    ]
    if not flag_columns:
        return df.withColumn(
            "dq_results",
            f.map_from_arrays(
                f.array().cast("array<string>"),
                f.array().cast("array<int>"),
            ),
        )

    entries = []
    for column in flag_columns:
        entries.extend(
            [
                f.lit(column.removeprefix("dq_")),
                f.col(column).cast("int"),
            ]
        )
    return df.withColumn("dq_results", f.create_map(*entries)).drop(*flag_columns)
