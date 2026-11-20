"""Adapters that run the shared geolocation checks against any facility profile.

The check engines operate directly on the facility/geo column names
(``facility_id_govt``, ``facility_name``, ``facility_level``, ``geo_latitude``,
``geo_longitude``, ...) and emit ``dq_*`` flags with those names, so no column or
flag aliasing is needed here.
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


def _enrich_admin_from_geo(df: sql.DataFrame, country_code: str) -> sql.DataFrame:
    """Derive admin1-4 and disputed-region columns from ``geo_latitude``/``geo_longitude``."""
    if {"geo_latitude", "geo_longitude"}.issubset(df.columns):
        for admin_level in ("admin1", "admin2", "admin3", "admin4"):
            df = add_admin_columns(
                df,
                country_code,
                admin_level,
                latitude_column="geo_latitude",
                longitude_column="geo_longitude",
            )
        df = add_disputed_region_column(df)
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
    return row_level_checks(
        enriched,
        silver=current,
        dq_context=DQContext(
            dq_mode=DQMode.MASTER,
            dataset_type="geolocation",
            country_code_iso3=country_code,
            dq_config_table=spec.dq_config_table,
            is_new_column="is_new_facility",
            values_domain=spec.dq_domains,
            values_range=spec.dq_ranges,
            duplicate_all_except_columns=tuple(
                column
                for column in spec.columns.all
                if column not in {spec.govt_id_column, spec.giga_id_column}
            ),
            unique_set_columns=(
                (
                    "facility_id_govt",
                    "facility_name",
                    "facility_level",
                    "location_id",
                ),
                ("facility_name", "facility_level", "location_id"),
                ("facility_level", "location_id"),
                ("location_id",),
            ),
        ),
        context=context,
        extra_outputs=extra_outputs,
    )


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
