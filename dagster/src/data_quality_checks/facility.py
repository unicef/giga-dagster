"""Adapters that run the mature geolocation checks against any facility profile.

The current check implementations use the historical school column names internally.
This module provides those names as temporary aliases and removes them afterwards,
allowing the check algorithms to be reused while their public input remains facility
specific.
"""

from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StringType, TimestampType

from dagster import OpExecutionContext
from src.data_quality_checks.create_update import (
    add_is_new_entity,
    enrich_with_current_values,
)
from src.data_quality_checks.dq_context import DQContext, DQMode
from src.data_quality_checks.utils import row_level_checks
from src.facilities import STABLE_FACILITY_ID_CONTRACT, FacilityGeolocationSpec
from src.settings import DeploymentEnvironment, settings
from src.spark.transform_functions import (
    add_admin_columns,
    add_connectivity_type_columns,
    add_disputed_region_column,
    get_country_rt_schools,
    map_govt_to_giga_columns,
    merge_connectivity_to_master,
)
from src.utils.school_registrations.common import (
    set_school_registration_verification_status,
)
from src.utils.spark import normalize_missing_value_strings

_CHECK_ALIASES = {
    "govt_id_column": "school_id_govt",
    "giga_id_column": "school_id_giga",
    "name_column": "school_name",
    "level_column": "education_level",
}


def _column_replacements(
    _spec: FacilityGeolocationSpec,
) -> tuple[tuple[str, str], ...]:
    """Map check-engine school names to canonical facility_*/geo_* report names.

    Always target the public schema names so DQ summaries and the PDF renderer
    share one contract for every facility_type. Longer keys are listed first so
    substring replacements (e.g. education_level_govt) win over shorter ones.
    """
    return (
        ("education_level_govt", "facility_level_govt"),
        ("education_level", "facility_level"),
        ("school_id_govt", "facility_id_govt"),
        ("school_id_giga", "facility_id_giga"),
        ("school_name", "facility_name"),
        ("school_density", "facility_density"),
        ("school_code", "facility_code"),
        ("latitude", "geo_latitude"),
        ("longitude", "geo_longitude"),
    )


def _dq_column_aliases(spec: FacilityGeolocationSpec) -> dict[str, str]:
    internal_columns = (
        "dq_duplicate-school_id_govt",
        "dq_duplicate-school_id_giga",
        "dq_is_null_mandatory-school_id_govt",
        "dq_is_null_optional-school_name",
        "dq_is_null_optional-education_level",
        "dq_is_null_optional-education_level_govt",
        "dq_is_null_optional-latitude",
        "dq_is_null_optional-longitude",
        "dq_is_not_alphanumeric-school_name",
        "dq_is_not_alphanumeric-education_level",
        "dq_is_not_alphanumeric-education_level_govt",
        "dq_precision-latitude",
        "dq_precision-longitude",
        "dq_is_invalid_range-latitude",
        "dq_is_invalid_range-longitude",
        "dq_is_not_numeric-latitude",
        "dq_is_not_numeric-longitude",
        ("dq_duplicate_set-school_id_govt_school_name_" "education_level_location_id"),
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


_GEO_CHECK_ALIASES = (
    ("geo_latitude", "latitude"),
    ("geo_longitude", "longitude"),
)


def _require_alias_column(
    df: sql.DataFrame,
    source_column: str,
    alias_column: str,
) -> tuple[sql.DataFrame, str | None]:
    """Copy ``source_column`` to ``alias_column`` when the alias is absent."""
    if alias_column in df.columns:
        return df, None
    if source_column not in df.columns:
        raise ValueError(
            f"Facility data is missing `{source_column}` required for `{alias_column}`"
        )
    return df.withColumn(alias_column, f.col(source_column)), alias_column


def _add_identity_check_aliases(
    df: sql.DataFrame,
    spec: FacilityGeolocationSpec,
) -> tuple[sql.DataFrame, list[str]]:
    added: list[str] = []
    for spec_attribute, check_column in _CHECK_ALIASES.items():
        source_column = getattr(spec, spec_attribute)
        if source_column == check_column:
            continue
        df, alias = _require_alias_column(df, source_column, check_column)
        if alias:
            added.append(alias)
    return df, added


def _add_geo_check_aliases(df: sql.DataFrame) -> tuple[sql.DataFrame, list[str]]:
    """Coordinate checks still hardcode latitude/longitude."""
    added: list[str] = []
    for public_col, check_col in _GEO_CHECK_ALIASES:
        df, alias = _require_alias_column(df, public_col, check_col)
        if alias:
            added.append(alias)
    return df, added


def _add_level_govt_check_alias(
    df: sql.DataFrame,
    spec: FacilityGeolocationSpec,
) -> tuple[sql.DataFrame, list[str]]:
    """Level-govt checks still hardcode education_level_govt."""
    if "education_level_govt" in df.columns:
        return df, []
    if "facility_level_govt" in df.columns:
        source = "facility_level_govt"
    elif spec.level_column in df.columns:
        source = spec.level_column
    else:
        raise ValueError(
            "Facility data is missing `facility_level_govt` required for "
            "`education_level_govt`"
        )
    df, alias = _require_alias_column(df, source, "education_level_govt")
    return df, [alias] if alias else []


def add_check_compatibility_columns(
    df: sql.DataFrame,
    spec: FacilityGeolocationSpec,
) -> tuple[sql.DataFrame, tuple[str, ...]]:
    """Add temporary historical aliases required by the geolocation check engine."""
    df, identity_aliases = _add_identity_check_aliases(df, spec)
    df, geo_aliases = _add_geo_check_aliases(df)
    df, level_aliases = _add_level_govt_check_alias(df, spec)
    return df, tuple(identity_aliases + geo_aliases + level_aliases)


def _enrich_admin_from_geo(df: sql.DataFrame, country_code: str) -> sql.DataFrame:
    """Alias geo_* → lat/lon for admin enrichment, then drop the temporary aliases."""
    added: list[str] = []
    for public_col, check_col in _GEO_CHECK_ALIASES:
        if check_col not in df.columns and public_col in df.columns:
            df = df.withColumn(check_col, f.col(public_col))
            added.append(check_col)
    if {"latitude", "longitude"}.issubset(df.columns):
        for admin_level in ("admin1", "admin2", "admin3", "admin4"):
            df = add_admin_columns(df, country_code, admin_level)
        df = add_disputed_region_column(df)
    for check_col in added:
        if check_col in df.columns:
            df = df.drop(check_col)
    return df


def _maybe_merge_connectivity_rt(
    df: sql.DataFrame,
    spec: FacilityGeolocationSpec,
    country_code: str,
    uploaded_columns: list[str],
    spark: SparkSession | None,
) -> sql.DataFrame:
    if not (
        spec.enable_connectivity
        and settings.DEPLOY_ENV != DeploymentEnvironment.LOCAL
        and spark is not None
    ):
        return df

    added_facility_id_govt = False
    if "facility_id_govt" not in df.columns:
        if spec.govt_id_column not in df.columns:
            raise ValueError(
                f"Facility data is missing `{spec.govt_id_column}` required "
                "for connectivity RT merge"
            )
        df = df.withColumn("facility_id_govt", f.col(spec.govt_id_column))
        added_facility_id_govt = True
    connectivity = get_country_rt_schools(spark, country_code)
    df = merge_connectivity_to_master(df, connectivity, uploaded_columns)
    if added_facility_id_govt and "facility_id_govt" in df.columns:
        df = df.drop("facility_id_govt")
    return df


def prepare_facility_bronze(
    df: sql.DataFrame,
    current: sql.DataFrame,
    spec: FacilityGeolocationSpec,
    country_code: str,
    context: OpExecutionContext = None,
    *,
    uploaded_columns: list[str] | None = None,
    source: str | None = None,
    spark: SparkSession | None = None,
) -> sql.DataFrame:
    """Apply shared facility bronze enrichment before DQ.

    Restores the school geolocation bronze steps that apply across facilities:
    govt→giga mappings, connectivity type derivation, optional RT merge, and
    connectivity_govt ingestion timestamps. School registration verification is
    gated to schools only.
    """
    uploaded_columns = list(uploaded_columns or df.columns)
    df = normalize_missing_value_strings(df)
    if spec.country_column not in df.columns:
        df = df.withColumn(spec.country_column, f.lit(country_code))

    df = map_govt_to_giga_columns(df, uploaded_columns)
    df = add_connectivity_type_columns(df, uploaded_columns)

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
    df = _enrich_admin_from_geo(df, country_code)

    if "connectivity_govt" in uploaded_columns:
        df = df.withColumn(
            "connectivity_govt_ingestion_timestamp",
            f.when(
                f.col("connectivity_govt").isNotNull(), f.current_timestamp()
            ).otherwise(f.lit(None).cast(TimestampType())),
        )

    df = _maybe_merge_connectivity_rt(df, spec, country_code, uploaded_columns, spark)

    if (
        spec.enable_registration_updates
        and spec.facility_type == "school"
        and source == "gigameter"
    ):
        df = set_school_registration_verification_status(df, uploaded_columns)

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
