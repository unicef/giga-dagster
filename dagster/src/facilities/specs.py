from dataclasses import dataclass, field

FACILITY_ID_COLUMN = "facility_id_giga"

SHARED_GEOSPATIAL_COLUMNS = (
    "latitude",
    "longitude",
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

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            _validate_name(value, field_name)
            if value.startswith("/") or value.endswith("/"):
                raise ValueError(f"{field_name} must be an ADLS-relative directory")

    def upload_path(self, upload_prefix: str = "raw/uploads") -> str:
        return f"{upload_prefix.rstrip('/')}/{self.upload_directory}"


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
            "latitude",
            "longitude",
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
    "school_id_giga",
    "school_id_govt",
    "school_name",
    "education_level",
    "country_code",
    *SHARED_GEOSPATIAL_COLUMNS,
)

SCHOOL_EXTENSION_COLUMNS = (
    "school_id_govt_type",
    "school_establishment_year",
    "school_area_type",
    "school_funding_type",
    "school_address",
    "is_school_open",
    "num_computers",
    "num_computers_desired",
    "num_teachers",
    "num_adm_personnel",
    "num_students",
    "num_classrooms",
    "num_latrines",
    "computer_lab",
    "electricity_availability",
    "electricity_type",
    "water_availability",
    "school_data_source",
    "school_data_collection_year",
    "school_data_collection_modality",
)

SCHOOL_FACILITY_SPEC = FacilityGeolocationSpec(
    facility_type="school",
    domain="school",
    dataset_type="geolocation",
    govt_id_column="school_id_govt",
    giga_id_column="school_id_giga",
    name_column="school_name",
    level_column="education_level",
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
        upload_directory="school-geolocation",
        dq_directory="school-geolocation",
        approval_directory="approved-row-ids/school-geolocation",
        delete_directory="delete-row-ids",
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
        approved_rows="manual_review_passed_rows",
        silver="silver",
        core="school_facility_core",
        extension="school_facility_details",
        rejected_rows="manual_review_failed_rows",
    ),
    dq_config_table="SchoolGeolocationMasterDQChecks",
    approval_label="School Geolocation",
    group_name="school_geolocation_data",
    enable_connectivity=True,
    enable_registration_updates=True,
)

HEALTH_CORE_COLUMNS = (
    "facility_id_giga",
    "facility_id_govt",
    "facility_name",
    "facility_level",
    "country_code",
    *SHARED_GEOSPATIAL_COLUMNS,
)

HEALTH_EXTENSION_COLUMNS = (
    "facility_type_govt",
    "ownership",
    "operator",
    "address",
    "is_operational",
    "bed_count",
    "staff_count",
    "services",
    "electricity_availability",
    "water_availability",
    "facility_data_source",
    "facility_data_collection_year",
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
        core=HEALTH_CORE_COLUMNS,
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
        upload_directory="health-facility-geolocation",
        dq_directory="health-facility-geolocation",
        approval_directory="approved-row-ids/health-facility-geolocation",
        delete_directory="delete-row-ids/health-facility-geolocation",
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
    dq_domains=(
        ("electricity_availability", ("yes", "no")),
        ("water_availability", ("yes", "no")),
    ),
    dq_ranges=(
        ("latitude", -90, 90),
        ("longitude", -180, 180),
        ("bed_count", 0, 1_000_000),
        ("staff_count", 0, 1_000_000),
        ("facility_data_collection_year", 1000, 9999),
    ),
)

# Short names are convenient for factories while the explicit names remain discoverable.
SCHOOL_SPEC = SCHOOL_FACILITY_SPEC
HEALTH_SPEC = HEALTH_FACILITY_SPEC
