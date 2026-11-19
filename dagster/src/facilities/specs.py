from dataclasses import dataclass, field

FACILITY_ID_COLUMN = "facility_id_giga"

# Upload/govt geospatial fields from the shared facilities core schema.
SHARED_GEO_UPLOAD_COLUMNS = (
    "geo_latitude",
    "geo_longitude",
    "geo_admin1_govt",
    "geo_admin2_govt",
)

# Pipeline-derived geospatial enrichments stored on facility_core.
SHARED_GEO_DERIVED_COLUMNS = (
    "admin1",
    "admin1_id_giga",
    "admin2",
    "admin2_id_giga",
    "admin3",
    "admin3_id_giga",
    "admin4",
    "admin4_id_giga",
    "disputed_region",
)

SHARED_GEOSPATIAL_COLUMNS = SHARED_GEO_UPLOAD_COLUMNS + SHARED_GEO_DERIVED_COLUMNS


def _validate_name(value: str, field_name: str) -> None:
    if not value or value.strip() != value:
        raise ValueError(f"{field_name} must be a non-empty, trimmed string")


def _validate_unique(values: tuple[str, ...], field_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must not contain duplicate values")


@dataclass(frozen=True, slots=True)
class FacilityColumnOwnership:
    """Source-column ownership for the combined approved facility record."""

    core: tuple[str, ...]
    extension: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_unique(self.core, "core columns")
        _validate_unique(self.extension, "extension columns")
        overlap = set(self.core).intersection(self.extension)
        if overlap:
            raise ValueError(
                "Core and extension columns must be disjoint; "
                f"overlap: {sorted(overlap)}"
            )

    @property
    def all(self) -> tuple[str, ...]:
        return self.core + self.extension


@dataclass(frozen=True, slots=True)
class FacilityTableNames:
    core_schema: str
    core_table: str
    extension_schema: str
    extension_table: str
    staging_schema: str
    silver_schema: str
    rejected_schema: str

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            _validate_name(getattr(self, field_name), field_name)


@dataclass(frozen=True, slots=True)
class FacilityPathNames:
    upload_directory: str
    dq_directory: str
    approval_directory: str
    delete_directory: str
    schema_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        for field_name in (
            "upload_directory",
            "dq_directory",
            "approval_directory",
            "delete_directory",
        ):
            value = getattr(self, field_name)
            _validate_name(value, field_name)
            if value.startswith("/") or value.endswith("/"):
                raise ValueError(f"{field_name} must be an ADLS-relative directory")
        if not self.schema_keys:
            raise ValueError("schema_keys must contain at least one key")
        for key in self.schema_keys:
            _validate_name(key, "schema_keys entry")
        _validate_unique(self.schema_keys, "schema_keys")

    def upload_path(self, upload_prefix: str = "raw/uploads") -> str:
        return f"{upload_prefix.rstrip('/')}/{self.upload_directory}"

    def matches_schema_key(self, schema_key: str | None) -> bool:
        return schema_key is not None and schema_key in self.schema_keys


@dataclass(frozen=True, slots=True)
class FacilityJobNames:
    automated_checks: str
    post_approval: str
    delete: str

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            _validate_name(getattr(self, field_name), field_name)


@dataclass(frozen=True, slots=True)
class FacilityAssetNames:
    raw: str
    bronze: str
    dq_results: str
    staging: str
    approved_rows: str
    silver: str
    core: str
    extension: str
    rejected_rows: str

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            _validate_name(getattr(self, field_name), field_name)


@dataclass(frozen=True, slots=True)
class FacilityGeolocationSpec:
    facility_type: str
    domain: str
    dataset_type: str
    govt_id_column: str
    giga_id_column: str
    name_column: str
    level_column: str
    country_column: str
    columns: FacilityColumnOwnership
    tables: FacilityTableNames
    paths: FacilityPathNames
    jobs: FacilityJobNames
    assets: FacilityAssetNames
    dq_config_table: str
    approval_label: str
    group_name: str
    metastore_schema_name: str | None = None
    enable_connectivity: bool = False
    enable_registration_updates: bool = False
    core_extra_aliases: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    dq_domains: tuple[tuple[str, tuple[object, ...]], ...] | None = None
    dq_ranges: tuple[tuple[str, float, float], ...] | None = None

    def __post_init__(self) -> None:
        string_fields = (
            "facility_type",
            "domain",
            "dataset_type",
            "govt_id_column",
            "giga_id_column",
            "name_column",
            "level_column",
            "country_column",
            "dq_config_table",
            "approval_label",
            "group_name",
        )
        for field_name in string_fields:
            _validate_name(getattr(self, field_name), field_name)
        if self.metastore_schema_name is not None:
            _validate_name(self.metastore_schema_name, "metastore_schema_name")

        required_core = {
            self.giga_id_column,
            self.govt_id_column,
            self.name_column,
            self.level_column,
            self.country_column,
            "geo_latitude",
            "geo_longitude",
        }
        missing = required_core.difference(self.columns.core)
        if missing:
            raise ValueError(
                f"Core ownership is missing required columns: {sorted(missing)}"
            )
        alias_targets = [target for _, target in self.core_extra_aliases]
        _validate_unique(tuple(alias_targets), "core alias targets")

    @property
    def domain_dataset_type(self) -> str:
        return f"{self.domain}-{self.dataset_type}"

    @property
    def metastore_schema(self) -> str:
        return self.metastore_schema_name or f"{self.domain}_{self.dataset_type}"

    @property
    def core_aliases(self) -> tuple[tuple[str, str], ...]:
        standard = (
            (self.giga_id_column, FACILITY_ID_COLUMN),
            (self.govt_id_column, "facility_id_govt"),
            (self.name_column, "facility_name"),
            (self.level_column, "facility_level"),
            (self.country_column, "country_code"),
        )
        return standard + self.core_extra_aliases


COMMON_CORE_COLUMNS = (
    # Pipeline-owned identity / classification
    "facility_id_giga",
    "facility_level",
    "country_code",
    # Shared upload profile
    "facility_id_govt",
    "facility_id_type_govt",
    "facility_id_building_govt",
    "facility_name",
    "facility_level_govt",
    "facility_ownership_type",
    "facility_area_type_govt",
    "facility_is_open",
    "facility_establishment_year",
    "facility_address",
    "facility_data_source",
    *SHARED_GEOSPATIAL_COLUMNS,
    # Connectivity and electricity
    "connectivity_govt",
    "connectivity_type_govt",
    "connectivity_down_speed_contracted",
    "connectivity_data_cap_contracted",
    "connectivity_cost_contracted",
    "connectivity_cost_contracted_ccy",
    "connectivity_provider",
    "connectivity_project",
    "electricity_availability",
    "electricity_type_govt",
    "electricity_reliability",
    "electricity_backup_system",
    # Buildings & amenities
    "facility_rooms_num",
    "facility_area_sqm",
    "facility_water_availability",
    # ICT / devices
    "device_availability",
    "device_computers_availability",
    "device_computers_num",
    "device_tablets_availability",
    "device_tablets_num",
    # People
    "staff_professional_num",
    "staff_professional_female_num",
    "staff_professional_male_num",
    "staff_professional_other_num",
    "staff_administrative_num",
    "staff_administrative_female_num",
    "staff_administrative_male_num",
    "staff_administrative_other_num",
    "refugee_camp",
    "refugee_population",
)

SCHOOL_EXTENSION_COLUMNS = (
    "computer_lab_availability",
    "students_num",
    "students_female_num",
    "students_male_num",
    "students_other_num",
)

_SHARED_DQ_DOMAINS = (
    ("connectivity_govt", ("yes", "no")),
    (
        "facility_level",
        (
            "Pre-Primary",
            "Primary",
            "Secondary",
            "Post-Secondary",
            "Pre-Primary and Primary",
            "Primary and Secondary",
            "Pre-Primary, Primary and Secondary",
            "Primary, Secondary and Post-Secondary",
        ),
    ),
    ("electricity_availability", ("yes", "no")),
    ("facility_water_availability", ("yes", "no")),
    ("facility_area_type_govt", ("urban", "rural")),
    ("facility_is_open", ("yes", "no")),
)

_SHARED_DQ_RANGES = (
    ("geo_latitude", -90, 90),
    ("geo_longitude", -180, 180),
    ("facility_establishment_year", 1000, 9999),
    ("connectivity_down_speed_contracted", 1, 500),
)

SCHOOL_FACILITY_SPEC = FacilityGeolocationSpec(
    facility_type="school",
    domain="school",
    dataset_type="geolocation",
    govt_id_column="facility_id_govt",
    giga_id_column="facility_id_giga",
    name_column="facility_name",
    level_column="facility_level",
    country_column="country_code",
    columns=FacilityColumnOwnership(
        core=COMMON_CORE_COLUMNS,
        extension=SCHOOL_EXTENSION_COLUMNS,
    ),
    tables=FacilityTableNames(
        core_schema="facility_core",
        core_table="facility_core",
        extension_schema="school_facility_details",
        extension_table="school_facility_details",
        staging_schema="school_geolocation_staging",
        silver_schema="school_geolocation_silver",
        rejected_schema="school_geolocation_rejected",
    ),
    paths=FacilityPathNames(
        upload_directory="facility-geolocation",
        dq_directory="school-geolocation",
        approval_directory="approved-row-ids/school-geolocation",
        delete_directory="delete-row-ids/school-geolocation",
        schema_keys=("schools",),
    ),
    jobs=FacilityJobNames(
        automated_checks="school_master_geolocation__automated_data_checks_job",
        post_approval="school_master_geolocation__post_manual_checks_job",
        delete="school_master_geolocation__admin_delete_rows_job",
    ),
    assets=FacilityAssetNames(
        raw="geolocation_raw",
        bronze="geolocation_bronze",
        dq_results="geolocation_data_quality_results",
        staging="geolocation_staging",
        # Unique keys so these do not collide with coverage's common assets.
        approved_rows="school_facility_manual_review_passed_rows",
        silver="school_geolocation_silver",
        core="school_facility_core",
        extension="school_facility_details",
        rejected_rows="school_facility_manual_review_failed_rows",
    ),
    dq_config_table="SchoolGeolocationMasterDQChecks",
    approval_label="School Geolocation",
    group_name="school_geolocation_data",
    enable_connectivity=True,
    enable_registration_updates=True,
    dq_domains=_SHARED_DQ_DOMAINS
    + (
        ("computer_lab_availability", ("yes", "no")),
        ("device_availability", ("yes", "no")),
        ("device_computers_availability", ("yes", "no")),
        (
            "facility_ownership_type",
            ("public", "private", "charitable", "others"),
        ),
    ),
    dq_ranges=_SHARED_DQ_RANGES
    + (
        ("device_computers_num", 0, 500),
        ("device_tablets_num", 0, 500),
        ("students_num", 0, 10_000),
        ("facility_rooms_num", 0, 2_000),
        ("facility_area_sqm", 0, 1_000_000),
    ),
)

HEALTH_EXTENSION_COLUMNS = (
    "facility_id_2_govt",
    "facility_id_2_type_govt",
    "facility_health_type_govt",
    "facility_health_care_type",
    "facility_health_service_type",
    "system_hmis",
    "system_hmis_used",
    "system_emr",
    "system_emr_used",
    "outpatients_avg",
    "outpatients_ref_period",
    "inpatients_avg",
    "inpatients_ref_period",
    "pop_catchment_govt",
)

HEALTH_FACILITY_SPEC = FacilityGeolocationSpec(
    facility_type="health",
    domain="health-facility",
    dataset_type="geolocation",
    govt_id_column="facility_id_govt",
    giga_id_column="facility_id_giga",
    name_column="facility_name",
    level_column="facility_level",
    country_column="country_code",
    columns=FacilityColumnOwnership(
        core=COMMON_CORE_COLUMNS,
        extension=HEALTH_EXTENSION_COLUMNS,
    ),
    tables=FacilityTableNames(
        core_schema="facility_core",
        core_table="facility_core",
        extension_schema="health_facility_details",
        extension_table="health_facility_details",
        staging_schema="health_facility_geolocation_staging",
        silver_schema="health_facility_geolocation_silver",
        rejected_schema="health_facility_geolocation_rejected",
    ),
    paths=FacilityPathNames(
        upload_directory="facility-geolocation",
        dq_directory="health-facility-geolocation",
        approval_directory="approved-row-ids/health-facility-geolocation",
        delete_directory="delete-row-ids/health-facility-geolocation",
        schema_keys=("health_facilities",),
    ),
    jobs=FacilityJobNames(
        automated_checks="health_facility_geolocation__automated_data_checks_job",
        post_approval="health_facility_geolocation__post_manual_checks_job",
        delete="health_facility_geolocation__admin_delete_rows_job",
    ),
    assets=FacilityAssetNames(
        raw="health_facility_geolocation_raw",
        bronze="health_facility_geolocation_bronze",
        dq_results="health_facility_geolocation_data_quality_results",
        staging="health_facility_geolocation_staging",
        approved_rows="health_facility_manual_review_passed_rows",
        silver="health_facility_geolocation_silver",
        core="health_facility_core",
        extension="health_facility_details",
        rejected_rows="health_facility_manual_review_failed_rows",
    ),
    dq_config_table="HealthFacilityGeolocationDQChecks",
    approval_label="Health Facility Geolocation",
    group_name="health_facility_geolocation_data",
    metastore_schema_name="health_facility_geolocation",
    dq_domains=_SHARED_DQ_DOMAINS
    + (
        ("system_hmis", ("yes", "no")),
        ("system_emr", ("yes", "no")),
    ),
    dq_ranges=_SHARED_DQ_RANGES
    + (
        ("outpatients_avg", 0, 1_000_000),
        ("inpatients_avg", 0, 1_000_000),
        ("pop_catchment_govt", 0, 10_000_000),
    ),
)

# Short names are convenient for factories while the explicit names remain discoverable.
SCHOOL_SPEC = SCHOOL_FACILITY_SPEC
HEALTH_SPEC = HEALTH_FACILITY_SPEC
