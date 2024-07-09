from datetime import date

from pydantic import BaseSettings


class Config(BaseSettings):
    SIMILARITY_RATIO_CUTOFF: float = 0.7
    date_today: date = date.today()
    current_year: int = date_today.year

    DATA_QUALITY_CHECKS_DESCRIPTIONS: list[dict[str, str]] = [
        {
            "assertion": "duplicate",
            "description": "Checks if column {} has a duplicate",
            "type": "duplicate_rows_checks",
        },
        {
            "assertion": "is_null_mandatory",
            "description": "Checks if non-nullable column {} is null",
            "type": "completeness_checks",
        },
        {
            "assertion": "is_null_optional",
            "description": "Checks if nullable column {} is null",
            "type": "completeness_checks",
        },
        {
            "assertion": "is_invalid_domain",
            "description": "Checks if column {} is within {set}",
            "type": "domain_checks",
        },
        {
            "assertion": "is_invalid_range",
            "description": "Checks if column {} is between {min} and {max}",
            "type": "range_checks",
        },
        {
            "assertion": "precision",
            "description": "Checks if column {} has at least {precision} decimal places",
            "type": "geospatial_checks",
        },
        {
            "assertion": "is_not_within_country",
            "description": "Checks if the coordinates is not within the country",
            "type": "geospatial_checks",
        },
        {
            "assertion": "duplicate_set",
            "description": "Checks if there are duplicates across these columns {}",
            "type": "duplicate_rows_checks",
        },
        {
            "assertion": "duplicate_all_except_school_code",
            "description": "Checks if there are duplicates across all columns except School ID",
            "type": "duplicate_rows_checks",
        },
        {
            "assertion": "duplicate_name_level_within_110m_radius",
            "description": "Checks if there are duplicates across name, level, lat_110, long_110",
            "type": "duplicate_rows_checks",
        },
        {
            "assertion": "duplicate_similar_name_same_level_within_110m_radius",
            "description": "Checks if there are duplicates across educational_level, lat_110, long_110 that has similar names as well.",
            "type": "duplicate_rows_checks",
        },
        {
            "assertion": "has_critical_error",
            "description": "Checks if the dataset contains a critical error.",
            "type": "critical_error_check",
        },
        {
            "assertion": "is_school_density_greater_than_5",
            "description": "Checks if the the school density within the area is greater than 5.",
            "type": "geospatial_checks",
        },
        {
            "assertion": "is_not_numeric",
            "description": "Checks if column {} is numeric.",
            "type": "format_validation_checks",
        },
        {
            "assertion": "is_not_alphanumeric",
            "description": "Checks if column {} is alphanumeric.",
            "type": "format_validation_checks",
        },
        {
            "assertion": "is_not_36_character_hash",
            "description": "Checks if school_id_giga is a 36 character hash.",
            "type": "format_validation_checks",
        },
        {
            "assertion": "is_sum_of_percent_not_equal_100",
            "description": "Checks if sum of percent_2G, percent_3G, percent_4G is equal to 100",
            "type": "custom_coverage_fb_check",
        },
        {
            "assertion": "is_string_more_than_255_characters",
            "description": "Checks if column {} is less than 255 characters.",
            "type": "format_validation_checks",
        },
        {
            "assertion": "column_relation_checks",
            "description": "Checks if column relationship between the following columns {} is as expected.",
            "type": "column_relation_checks",
        },
        {
            "assertion": "is_not_create",
            "description": "Checks if the school entered is already in the system. If it is already in the system, it cannot be recreated.",
            "type": "is_not_create",
        },
        {
            "assertion": "is_not_update",
            "description": "Checks if the school entered is not yet in the system. If it is not yet in the system, it cannot be updated.",
            "type": "is_not_update",
        },
    ]

    DATA_TYPES: set[tuple[str, str]] = {  ## @RENZ are we not pulling this from schema?
        ("cellular_coverage_availability", "STRING"),
        ("cellular_coverage_type", "STRING"),
        ("fiber_node_distance", "DOUBLE"),
        ("microwave_node_distance", "DOUBLE"),
        ("schools_within_1km", "INT"),
        ("schools_within_2km", "INT"),
        ("schools_within_3km", "INT"),
        ("nearest_NR_distance", "DOUBLE"),
        ("nearest_LTE_distance", "DOUBLE"),
        ("nearest_UMTS_distance", "DOUBLE"),
        ("nearest_GSM_distance", "DOUBLE"),
        ("pop_within_1km", "LONG"),
        ("pop_within_2km", "LONG"),
        ("pop_within_3km", "LONG"),
        ("connectivity_govt_collection_year", "INT"),
        ("connectivity_govt", "STRING"),
        ("school_id_giga", "STRING"),
        ("school_id_govt", "STRING"),
        ("school_name", "STRING"),
        ("school_establishment_year", "INT"),
        ("latitude", "DOUBLE"),
        ("longitude", "DOUBLE"),
        ("education_level", "STRING"),
        ("download_speed_contracted", "DOUBLE"),
        ("connectivity_type_govt", "STRING"),
        ("admin1", "STRING"),
        ("admin1_id_giga", "STRING"),
        ("admin2", "STRING"),
        ("admin2_id_giga", "STRING"),
        ("school_area_type", "STRING"),
        ("school_funding_type", "STRING"),
        ("num_computers", "INT"),
        ("num_computers_desired", "INT"),
        ("num_teachers", "INT"),
        ("num_adm_personnel", "INT"),
        ("num_students", "INT"),
        ("num_classrooms", "INT"),
        ("num_latrines", "INT"),
        ("computer_lab", "STRING"),
        ("electricity_availability", "STRING"),
        ("electricity_type", "STRING"),
        ("water_availability", "STRING"),
        ("school_data_source", "STRING"),
        ("school_data_collection_year", "INT"),
        ("school_data_collection_modality", "STRING"),
        ("connectivity_govt_ingestion_timestamp", "TIMESTAMP"),
        ("school_location_ingestion_timestamp", "TIMESTAMP"),
        ("disputed_region", "STRING"),
        ("connectivity", "STRING"),
        ("connectivity_RT", "STRING"),
        ("connectivity_RT_datasource", "STRING"),
        ("connectivity_RT_ingestion_timestamp", "TIMESTAMP"),
        ("school_id_giga", "STRING"),
        ("pop_within_10km", "LONG"),
        ("nearest_school_distance", "DOUBLE"),
        ("schools_within_10km", "INT"),
        ("nearest_NR_id", "STRING"),
        ("nearest_LTE_id", "STRING"),
        ("nearest_UMTS_id", "STRING"),
        ("nearest_GSM_id", "STRING"),
        ("education_level_govt", "STRING"),
        ("download_speed_govt", "DOUBLE"),
        ("school_id_govt_type", "STRING"),
        ("school_address", "STRING"),
        ("is_school_open", "STRING"),
        ("education_level", "STRING"),
        ("percent_2G", "DOUBLE"),
        ("percent_3G", "DOUBLE"),
        ("percent_4G", "DOUBLE"),
        ("2G", "INT"),
        ("3G", "INT"),
        ("4G", "INT"),
        # qos
        ("timestamp", "TIMESTAMP"),
        ("country_id", "STRING"),
        ("speed_upload", "DOUBLE"),
        ("speed_download", "DOUBLE"),
        ("roundtrip_time", "DOUBLE"),
        ("jitter_upload", "DOUBLE"),
        ("jitter_download", "DOUBLE"),
        ("rtt_packet_loss_pct", "DOUBLE"),
        ("provider", "STRING"),
        ("latency", "DOUBLE"),
        ("report_id", "STRING"),
        ("agent_id", "STRING"),
    }

    UNIQUE_COLUMNS_MASTER: list[str] = ["school_id_govt", "school_id_giga"]

    UNIQUE_COLUMNS_REFERENCE: list[str] = ["school_id_giga"]

    UNIQUE_COLUMNS_GEOLOCATION: list[str] = ["school_id_govt", "school_id_giga"]

    UNIQUE_COLUMNS_COVERAGE: list[str] = ["school_id_giga"]

    UNIQUE_COLUMNS_COVERAGE_FB: list[str] = ["school_id_giga"]

    UNIQUE_COLUMNS_COVERAGE_ITU: list[str] = ["school_id_giga"]

    UNIQUE_COLUMNS_QOS: list[str] = ["school_id_giga"]

    NONEMPTY_COLUMNS_MASTER: list[str] = [
        "school_id_giga",
        "school_id_govt",
        "school_name",
        "longitude",
        "latitude",
        "education_level",
        "admin1",
        "admin2",
    ]

    NONEMPTY_COLUMNS_REFERENCE: list[str] = [
        "school_id_giga",
        "education_level_govt",
    ]

    NONEMPTY_COLUMNS_GEOLOCATION: list[str] = [
        "school_id_govt",
    ]

    NONEMPTY_COLUMNS_COVERAGE: list[str] = [
        "school_id_giga",
        "cellular_coverage_availability",
        "cellular_coverage_type",
    ]

    NONEMPTY_COLUMNS_COVERAGE_FB: list[str] = [
        "school_id_giga",
        "percent_2G",
        "percent_3G",
        "percent_4G",
    ]

    NONEMPTY_COLUMNS_COVERAGE_ITU: list[str] = [
        "school_id_giga",
        "2G",
        "3G",
        "4G",
        "fiber_node_distance",
        "microwave_node_distance",
        "nearest_school_distance",
        "schools_within_1km",
        "schools_within_2km",
        "schools_within_3km",
        "schools_within_10km",
        "nearest_NR_id",
        "nearest_NR_distance",
        "nearest_LTE_id",
        "nearest_LTE_distance",
        "nearest_UMTS_id",
        "nearest_UMTS_distance",
        "nearest_GSM_id",
        "nearest_GSM_distance",
        "pop_within_1km",
        "pop_within_2km",
        "pop_within_3km",
        "pop_within_10km",
    ]

    NONEMPTY_COLUMNS_QOS: list[str] = [
        "school_id_giga",
        # "school_id_govt",
        # "timestamp",
        # "country_id",
        # "speed_upload",
        # "speed_download",
        # "provider",
    ]

    @property
    def NONEMPTY_COLUMNS_ALL(self) -> list[str]:
        return [
            *self.NONEMPTY_COLUMNS_MASTER,
            *self.NONEMPTY_COLUMNS_REFERENCE,
            *self.NONEMPTY_COLUMNS_GEOLOCATION,
            *self.NONEMPTY_COLUMNS_COVERAGE,
            *self.NONEMPTY_COLUMNS_QOS,
        ]

    VALUES_DOMAIN_MASTER: dict[str, list[str]] = {
        "cellular_coverage_availability": ["yes", "no"],
        "cellular_coverage_type": ["2G", "3G", "4G", "5G", "no coverage"],
        "connectivity_govt": ["yes", "no"],
        "education_level": [
            "Pre-Primary",
            "Primary",
            "Secondary",
            "Post-Secondary",
            "Pre-Primary and Primary",
            "Primary and Secondary",
            "Pre-Primary, Primary and Secondary",
            "Primary, Secondary and Post-Secondary",
        ],
        "connectivity_type_govt": [
            "fiber",
            "xdsl",
            "wired",
            "cellular",
            "p2mp wireless",
            "p2p wireless",
            "satellite",
            "other",
            "copper",
            "coaxial",
            "unknown",
        ],
        "school_area_type": ["urban", "rural"],
        "computer_lab": ["yes", "no"],
        "electricity_availability": ["yes", "no"],
        "electricity_type": [
            "electrical grid",
            "diesel generator",
            "solar power station",
            "other",
        ],
        "water_availability": ["yes", "no"],
        "school_data_collection_modality": ["online", "in-person", "phone", "other"],
        "school_funding_type": ["public", "private", "charitable", "others"],
    }

    VALUES_DOMAIN_REFERENCE: dict[str, list[str]] = {
        "is_school_open": ["yes", "no"],
    }

    VALUES_DOMAIN_GEOLOCATION: dict[str, list[str]] = {
        "connectivity_govt": ["yes", "no"],
        "education_level": [
            "Pre-Primary",
            "Primary",
            "Secondary",
            "Post-Secondary",
            "Pre-Primary and Primary",
            "Primary and Secondary",
            "Pre-Primary, Primary and Secondary",
            "Primary, Secondary and Post-Secondary",
        ],
        "connectivity_type_govt": [
            "fiber",
            "xdsl",
            "wired",
            "cellular",
            "p2mp wireless",
            "p2p wireless",
            "satellite",
            "other",
            "copper",
            "coaxial",
            "unknown",
        ],
        "school_area_type": ["urban", "rural"],
        "computer_lab": ["yes", "no"],
        "electricity_availability": ["yes", "no"],
        "electricity_type": [
            "electrical grid",
            "diesel generator",
            "solar power station",
            "other",
        ],
        "water_availability": ["yes", "no"],
        "school_data_collection_modality": ["online", "in-person", "phone", "other"],
        "school_funding_type": ["public", "private", "charitable", "others"],
    }

    VALUES_DOMAIN_COVERAGE: dict[str, list[str]] = {
        "cellular_coverage_availability": ["yes", "no"],
        "cellular_coverage_type": ["2G", "3G", "4G", "5G", "no coverage"],
    }

    VALUES_DOMAIN_COVERAGE_ITU: dict[str, list[str]] = {
        "2G": [1, 0],
        "3G": [1, 0],
        "4G": [1, 0],
    }

    @property
    def VALUES_DOMAIN_ALL(self) -> dict[str, list[str]]:
        return {
            **self.VALUES_DOMAIN_MASTER,
            **self.VALUES_DOMAIN_REFERENCE,
            **self.VALUES_DOMAIN_GEOLOCATION,
            **self.VALUES_DOMAIN_COVERAGE,
            **self.VALUES_DOMAIN_COVERAGE_ITU,
        }

    @property
    def VALUES_RANGE_MASTER(self) -> dict[str, dict[str, int]]:
        return {
            "fiber_node_distance": {"min": 0, "max": 10000000},
            "microwave_node_distance": {"min": 0, "max": 10000000},
            "schools_within_1km": {"min": 0, "max": 20},
            "schools_within_2km": {"min": 0, "max": 40},
            "schools_within_3km": {"min": 0, "max": 60},
            "school_establishment_year": {"min": 1000, "max": self.current_year},
            "latitude": {"min": -90, "max": 90},
            "longitude": {"min": -180, "max": 180},
            "download_speed_contracted": {"min": 1, "max": 500},
            "num_computers": {"min": 0, "max": 500},
            "num_computers_desired": {"min": 0, "max": 1000},
            "num_teachers": {"min": 0, "max": 200},
            "num_adm_personnel": {"min": 0, "max": 200},
            "num_students": {"min": 0, "max": 10000},
            "num_classrooms": {"min": 0, "max": 200},
            "num_latrines": {"min": 0, "max": 200},
            "school_data_collection_year": {"min": 1000, "max": self.current_year},
        }

    @property
    def VALUES_RANGE_REFERENCE(self) -> dict[str, dict[str, int]]:
        return {
            "nearest_school_distance": {"min": 0, "max": 10000000},
            "schools_within_10km": {"min": 0, "max": 100},
            "download_speed_govt": {"min": 1, "max": 200},
        }

    @property
    def VALUES_RANGE_GEOLOCATION(self) -> dict[str, dict[str, int]]:
        return {
            "school_establishment_year": {"min": 1000, "max": self.current_year},
            "latitude": {"min": -90, "max": 90},
            "longitude": {"min": -180, "max": 180},
            "download_speed_contracted": {"min": 1, "max": 500},
            "num_computers": {"min": 0, "max": 500},
            "num_computers_desired": {"min": 0, "max": 1000},
            "num_teachers": {"min": 0, "max": 200},
            "num_adm_personnel": {"min": 0, "max": 200},
            "num_students": {"min": 0, "max": 10000},
            "num_classrooms": {"min": 0, "max": 200},
            "num_latrines": {"min": 0, "max": 200},
            "school_data_collection_year": {"min": 1000, "max": self.current_year},
            "download_speed_govt": {"min": 1, "max": 200},
        }

    VALUES_RANGE_COVERAGE: dict[str, dict[str, int]] = {
        "fiber_node_distance": {"min": 0, "max": 10000000},
        "microwave_node_distance": {"min": 0, "max": 10000000},
        "schools_within_1km": {"min": 0, "max": 20},
        "schools_within_2km": {"min": 0, "max": 40},
        "schools_within_3km": {"min": 0, "max": 60},
        "nearest_school_distance": {"min": 0, "max": 10000000},
        "schools_within_10km": {"min": 0, "max": 100},
    }

    @property
    def VALUES_RANGE_COVERAGE_ITU(self) -> dict[str, dict[str, int]]:
        return self.VALUES_RANGE_COVERAGE

    VALUES_RANGE_CRITICAL: dict[str, dict[str, int]] = {
        "latitude": {"min": -90, "max": 90},
        "longitude": {"min": -180, "max": 180},
    }

    @property
    def VALUES_RANGE_ALL(self) -> dict[str, dict[str, int]]:
        return {
            **self.VALUES_RANGE_MASTER,
            **self.VALUES_RANGE_REFERENCE,
            **self.VALUES_RANGE_GEOLOCATION,
            **self.VALUES_RANGE_COVERAGE,
        }

    UNIQUE_SET_COLUMNS: list[list[str]] = [
        ["school_id_govt", "school_name", "education_level", "location_id"],
        ["school_name", "education_level", "location_id"],
        ["education_level", "location_id"],
    ]

    PRECISION: dict[str, dict[str, int]] = {
        "latitude": {"min": 5},
        "longitude": {"min": 5},
    }

    # Coverage Column Configs

    # Lower Columns
    ITU_COLUMNS_TO_RENAME: list[str] = [
        "Schools_within_1km",
        "Schools_within_2km",
        "Schools_within_3km",
        "Schools_within_10km",
    ]

    # Columns To Keep From Dataset
    FB_COLUMNS: list[str] = [
        "school_id_giga",
        "2G_coverage",
        "3G_coverage",
        "4G_coverage",
    ]

    ITU_COLUMNS: list[str] = [
        "school_id_giga",
        "2G_coverage",
        "3G_coverage",
        "4G_coverage",
        "fiber_node_distance",
        "microwave_node_distance",
        "nearest_school_distance",
        "schools_within_1km",
        "schools_within_2km",
        "schools_within_3km",
        "schools_within_10km",
        "nearest_NR_id",
        "nearest_NR_distance",
        "nearest_LTE_id",
        "nearest_LTE_distance",
        "nearest_UMTS_id",
        "nearest_UMTS_distance",
        "nearest_GSM_id",
        "nearest_GSM_distance",
        "pop_within_1km",
        "pop_within_2km",
        "pop_within_3km",
        "pop_within_10km",
    ]

    COV_COLUMN_MERGE_LOGIC: list[str] = [
        "cellular_coverage_availability",
        "cellular_coverage_type",
    ]

    COLUMNS_EXCEPT_SCHOOL_ID_GEOLOCATION: list[str] = [
        # "school_id_giga",
        # "school_id_govt",
        "school_name",
        "school_establishment_year",
        "latitude",
        "longitude",
        "education_level",
        "education_level_govt",
        "connectivity_govt",
        "connectivity_govt_ingestion_timestamp",
        "connectivity_govt_collection_year",
        "download_speed_govt",
        "download_speed_contracted",
        "connectivity_type_govt",
        "admin1",
        "admin2",
        "school_area_type",
        "school_funding_type",
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
        "school_id_govt_type",
        "school_address",
        "is_school_open",
        "school_location_ingestion_timestamp",
    ]

    COLUMNS_EXCEPT_SCHOOL_ID_MASTER: list[str] = [
        "cellular_coverage_availability",
        "cellular_coverage_type",
        "fiber_node_distance",
        "microwave_node_distance",
        "schools_within_1km",
        "schools_within_2km",
        "schools_within_3km",
        "nearest_NR_distance",
        "nearest_LTE_distance",
        "nearest_UMTS_distance",
        "nearest_GSM_distance",
        "pop_within_1km",
        "pop_within_2km",
        "pop_within_3km",
        "connectivity_govt_collection_year",
        "connectivity_govt",
        # "school_id_giga",
        # "school_id_govt",
        "school_name",
        "school_establishment_year",
        "latitude",
        "longitude",
        "education_level",
        "download_speed_contracted",
        "connectivity_type_govt",
        "admin1",
        "admin1_id_giga",
        "admin2",
        "admin2_id_giga",
        "school_area_type",
        "school_funding_type",
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
        "connectivity_govt_ingestion_timestamp",
        "school_location_ingestion_timestamp",
        "disputed_region",
        "connectivity",
        "connectivity_RT",
        "connectivity_RT_datasource",
        "connectivity_RT_ingestion_timestamp",
    ]


config = Config()
# notes
# admin 1 admin 2 connectivity_RT nonnullable
# admin 1 admin 2 connectivity_RT required
# valid lat lon = decimal check and range check
# added country grouping in checking for duplicates because appended all countries in one dataset
# added educational level "Primary, Secondary and Post-Secondary"
# updated connectivity types vs given configs
# checks for numeric types
# checks for alphabets


# added transforms for standardization of educ types (unmapped = "Other")
# edul_fix={ np.nan : 'Other',
#             'Other' : 'Other',
#             'Pre-Primary And Primary And Secondary': 'Pre-Primary, Primary and Secondary',
#             'Primary, Secondary And Post-Secondary':'Primary, Secondary and Post-Secondary',
#             'Pre-Primary, Primary, And Secondary':'Pre-Primary, Primary and Secondary',
#             'Basic':'Primary',
#             'Basic And Secondary':'Primary and Secondary',
#             "Pre-Primary":"Pre-Primary",
#             "Primary":"Primary",
#             "Secondary":"Secondary",
#             "Post-Secondary":"Post-Secondary",
#             "Pre-Primary And Primary":"Pre-Primary and Primary",
#             "Primary And Secondary":"Primary and Secondary",
#             "Pre-Primary, Primary And Secondary": "Pre-Primary, Primary and Secondary"
#           }
# added transforms to normalize typos in type_connectivity (connectivity_type_govt)
# paterns={
# 'Fibre': ['fiber*', 'fibre*', 'fibra*', 'ftt*', 'fttx*', 'ftth*', 'fttp*', 'gpon*', 'epon*', 'fo*', 'Фибер*', 'optic*']
# ,'Copper':	['adsl*' , 'dsl*', 'copper*', 'hdsl*', 'vdsl*']
# ,'Coaxial': 	['coax*', 'coaxial*' ]
# ,'Cellular':	['cell*', 'cellular*', 'celular*', '2g*', '3g*', '4g*', '5g*', 'lte*', 'gsm*', 'umts*', 'cdma*', 'mobile*', 'mobie*']
# ,'P2P': 	['p2p*', 'radio*', 'microwave*', 'ptmp*' ,'micro.wave*']
# ,'Satellite':	['satellite*', 'satelite*', 'vsat*' , 'geo*', 'leo*']
# ,'Other':	['TVWS*', 'other*', 'ethernet*']
# ,'Unknown': 	['unknown*', 'null*', 'nan*', 'n/a*']
# }
# transforms for school region normalizing
# urban_school_region = ['Urban', 'Urbaine', 'Urbano', 'urban']
# rural_school_region = ['Rural', 'Rurale', 'Urbaine', 'rural']
# transforms for school type
# public_labels = ['Estadual','Municipal','Federal', 'Government', 'Public', 'Pubic', 'public']
# private_labels = ['Private', 'Private ']
