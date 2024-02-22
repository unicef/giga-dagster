import datetime

SIMILARITY_RATIO_CUTOFF = 0.7


# Data Quality Checks Descriptions

CONFIG_DATA_QUALITY_CHECKS_DESCRIPTIONS = [
    {
        "assertion": "duplicate", 
        "description": "Checks if column {} has a duplicate",
        "type": "duplicate_rows_checks"
    },
    {
        "assertion": "isnullmandatory", 
        "description": "Checks if non-nullable column {} is null",
        "type": "completeness_checks"
    },
    {
        "assertion": "isnulloptional", 
        "description": "Checks if nullable column {} is null",
        "type": "completeness_checks"
    },
    {
        "assertion": "isinvaliddomain", 
        "description": "Checks if column {} is within {set}",
        "type": "domain_checks"
    },
    {
        "assertion": "isinvalidrange", 
        "description": "Checks if column {} is between {min} and {max}",
        "type": "range_checks"
    },
    {
        "assertion": "precision", 
        "description": "Checks if column {} has at least {precision} decimal places",
        "type": "geospatial_checks"
    },
    {
        "assertion": "is_not_within_country", 
        "description": "Checks if the coordinates is not within the country",
        "type": "geospatial_checks"
    },
    {
        "assertion": "duplicateset", 
        "description": "Checks if there are duplicates across these columns {}",
        "type": "duplicate_rows_checks"
    },
    {
        "assertion": "duplicate_all_except_school_code", 
        "description": "Checks if there are duplicates across all columns except School ID",
        "type": "duplicate_rows_checks"
    },
    {
        "assertion": "duplicate_name_level_within_110m_radius", 
        "description": "Checks if there are duplicates across name, level, lat_110, long_110",
        "type": "duplicate_rows_checks"
    },
    {
        "assertion": "duplicate_similar_name_same_level_within_110m_radius", 
        "description": "Checks if there are duplicates across educational_level, lat_110, long_110 that has similar names as well.",
        "type": "duplicate_rows_checks"
    },
    {
        "assertion": "has_critical_error", 
        "description": "Checks if the dataset contains a critical error.",
        "type": "critical_error_check"
    },
    {
        "assertion": "is_school_density_greater_than_5", 
        "description": "Checks if the the school density within the area is greater than 5.",
        "type": "geospatial_checks"
    },
    {
        "assertion": "isnotnumeric", 
        "description": "Checks if column {} is numeric.",
        "type": "format_validation_checks"
    },
    {
        "assertion": "isnotalphanumeric", 
        "description": "Checks if column {} is alphanumeric.",
        "type": "format_validation_checks"
    },
]

# Data Types
CONFIG_DATA_TYPES = {
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
}

# Single Column w/ parameters
date_today = datetime.date.today()
current_year = date_today.year

# Single column checks
CONFIG_UNIQUE_COLUMNS_MASTER = ["school_id_govt", "school_id_giga"]

CONFIG_UNIQUE_COLUMNS_REFERENCE = ["school_id_giga"]

CONFIG_UNIQUE_COLUMNS_GEOLOCATION = ["school_id_govt", "school_id_giga"]

CONFIG_UNIQUE_COLUMNS_COVERAGE = ["school_id_giga"]

CONFIG_UNIQUE_COLUMNS_CRITICAL = ["school_id_govt", "school_id_giga"]

CONFIG_NONEMPTY_COLUMNS_MASTER = [
    "school_id_giga",
    "school_id_govt",
    "school_name",
    "longitude",
    "latitude",
    "education_level",
    "admin1",
    "admin2",
]

CONFIG_NONEMPTY_COLUMNS_REFERENCE = [
    "school_id_giga",
    "education_level_govt",
    "school_id_govt_type",
]

CONFIG_NONEMPTY_COLUMNS_GEOLOCATION = [
    "school_id_giga",
    "school_id_govt",
    "school_name",
    "longitude",
    "latitude",
    "education_level",
    "education_level_govt",
    "school_id_govt_type",
    "admin1",
    "admin2",
]

CONFIG_NONEMPTY_COLUMNS_COVERAGE = [
    "school_id_giga",
]

CONFIG_NONEMPTY_COLUMNS_CRITICAL = [
    "school_name",
    "longitude",
    "latitude",
]

CONFIG_NONEMPTY_COLUMNS_ALL = CONFIG_NONEMPTY_COLUMNS_MASTER + CONFIG_NONEMPTY_COLUMNS_REFERENCE + CONFIG_NONEMPTY_COLUMNS_GEOLOCATION + CONFIG_NONEMPTY_COLUMNS_COVERAGE + CONFIG_NONEMPTY_COLUMNS_CRITICAL


CONFIG_VALUES_DOMAIN_MASTER = {
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
    "school_data_collection_modality": [
        "online", 
        "in-person", 
        "phone", 
        "other"
    ],
}

CONFIG_VALUES_DOMAIN_REFERENCE = {
    "is_school_open": ["yes", "no"],
}
CONFIG_VALUES_DOMAIN_GEOLOCATION = {
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
    "school_data_collection_modality": [
        "online", 
        "in-person", 
        "phone", 
        "other"
    ],
   
}

CONFIG_VALUES_DOMAIN_COVERAGE = {
    "cellular_coverage_availability": ["yes", "no"],
    "cellular_coverage_type": ["2G", "3G", "4G", "5G", "no coverage"],
}

CONFIG_VALUES_DOMAIN_ALL = CONFIG_VALUES_DOMAIN_MASTER | CONFIG_VALUES_DOMAIN_REFERENCE | CONFIG_VALUES_DOMAIN_GEOLOCATION | CONFIG_VALUES_DOMAIN_COVERAGE


# For RANGE Data Quality Checks
CONFIG_VALUES_RANGE_MASTER = {
    "fiber_node_distance": {"min": 0, "max": 10000000},
    "microwave_node_distance": {"min": 0, "max": 10000000},
    "schools_within_1km": {"min": 0, "max": 20},
    "schools_within_2km": {"min": 0, "max": 40},
    "schools_within_3km": {"min": 0, "max": 60},
    "school_establishment_year": {"min": 1000, "max": current_year},
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
    "school_data_collection_year": {"min": 1000, "max": current_year},
}
CONFIG_VALUES_RANGE_REFERENCE = {
    "nearest_school_distance": {"min": 0, "max": 10000000},
    "schools_within_10km": {"min": 0, "max": 100},
    "download_speed_govt": {"min": 1, "max": 200},
}
CONFIG_VALUES_RANGE_GEOLOCATION = {
    "school_establishment_year": {"min": 1000, "max": current_year},
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
    "school_data_collection_year": {"min": 1000, "max": current_year},
    "download_speed_govt": {"min": 1, "max": 200},
}

CONFIG_VALUES_RANGE_COVERAGE = {
    "fiber_node_distance": {"min": 0, "max": 10000000},
    "microwave_node_distance": {"min": 0, "max": 10000000},
    "schools_within_1km": {"min": 0, "max": 20},
    "schools_within_2km": {"min": 0, "max": 40},
    "schools_within_3km": {"min": 0, "max": 60},
    "nearest_school_distance": {"min": 0, "max": 10000000},
    "schools_within_10km": {"min": 0, "max": 100},
}

CONFIG_VALUES_RANGE_CRITICAL = {
    "latitude": {"min": -90, "max": 90},
    "longitude": {"min": -180, "max": 180},
}

CONFIG_VALUES_RANGE_ALL = CONFIG_VALUES_RANGE_MASTER | CONFIG_VALUES_RANGE_REFERENCE | CONFIG_VALUES_RANGE_GEOLOCATION | CONFIG_VALUES_RANGE_COVERAGE

CONFIG_UNIQUE_SET_COLUMNS = [
    ["school_id_govt", "school_name", "education_level", "location_id"],
    ["school_name", "education_level", "location_id"],
    ["education_level", "location_id"],
    # school name educ lat 110 lon 110
    # similar school name educ lat 110 lon 110
]

CONFIG_NOT_SIMILAR_COLUMN = ["school_name"]
CONFIG_FIVE_DECIMAL_PLACES = ["latitude", "longitude"]

# Multi-column checks

CONFIG_COLUMN_SUM = [
    ["student_count_girls", "student_count_boys", "student_count_others"]
]


CONFIG_VALUES_RANGE_PRIO = {
    "download_speed_govt": {"min": 1, "max": 200},
    # "school_density": {"min": 0, "max": 5},
}

# For GOVERNMENT SCHOOL COLUMNS




CONFIG_VALUES_TYPE = [{"column": "school_id_govt", "type": "int64"}]

# Column Pairs
CONFIG_PAIR_AVAILABILITY = [
    {"availability_column": "internet_availability", "value_column": "internet_type"},
    {
        "availability_column": "internet_availability",
        "value_column": "download_speed_govt",
    },
]

CONFIG_PRECISION = {
    "latitude": {"min": 5},
    "longitude": {"min": 5},
}

# Geolocation Column Configs
CONFIG_COLUMN_RENAME_GEOLOCATION = {
    # raw, delta_col
    ("school_id", "school_id_govt"),
    ("school_name", "school_name"),
    ("school_id_gov_type", "school_id_govt_type"),
    ("school_establishment_year", "school_establishment_year"),
    ("latitude", "latitude"),
    ("longitude", "longitude"),
    ("education_level", "education_level"),
    ("education_level_govt", "education_level_govt"),
    ("internet_availability", "connectivity_govt"),
    ("connectivity_govt_ingestion_timestamp", "connectivity_govt_ingestion_timestamp"),
    ("connectivity_govt_collection_year", "connectivity_govt_collection_year"),
    ("internet_speed_mbps", "download_speed_govt"),
    ("download_speed_contracted", "download_speed_contracted"),
    ("internet_type", "connectivity_type_govt"),
    ("admin1", "admin1"),
    ("admin2", "admin2"),
    ("school_region", "school_area_type"),
    ("school_funding_type", "school_funding_type"),
    ("computer_count", "num_computers"),
    ("desired_computer_count", "num_computers_desired"),
    ("teacher_count", "num_teachers"),
    ("adm_personnel_count", "num_adm_personnel"),
    ("student_count", "num_students"),
    ("classroom_count", "num_classrooms"),
    ("num_latrines", "num_latrines"),
    ("computer_lab", "computer_lab"),
    ("electricity", "electricity_availability"),
    ("electricity_type", "electricity_type"),
    ("water", "water_availability"),
    ("school_data_source", "school_data_source"),
    ("school_data_collection_year", "school_data_collection_year"),
    ("school_data_collection_modality", "school_data_collection_modality"),
    ("address", "school_address"),
    ("is_open", "is_school_open"),
    ("school_location_ingestion_timestamp", "school_location_ingestion_timestamp"),
}


# Coverage Column Configs

# Lower Columns
CONFIG_ITU_COLUMNS_TO_RENAME = [
    "Schools_within_1km",
    "Schools_within_2km",
    "Schools_within_3km",
    "Schools_within_10km",
]

# Columns To Keep From Dataset
CONFIG_FB_COLUMNS = ["school_id_giga", "2G_coverage", "3G_coverage", "4G_coverage"]
CONFIG_ITU_COLUMNS = [
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
CONFIG_COV_COLUMNS = [
    "school_id_giga",
    "cellular_coverage_availability",
    "cellular_coverage_type",
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

CONFIG_COV_COLUMN_RENAME = {
    ("giga_id_school", "school_id_giga"),
    ("coverage_availability", "cellular_coverage_availability"),
    ("coverage_type", "cellular_coverage_type"),
    ("fiber_node_distance", "fiber_node_distance"),
    ("microwave_node_distance", "microwave_node_distance"),
    ("nearest_school_distance", "nearest_school_distance"),
    ("schools_within_1km", "schools_within_1km"),
    ("schools_within_2km", "schools_within_2km"),
    ("schools_within_3km", "schools_within_3km"),
    ("schools_within_10km", "schools_within_10km"),
    ("nearest_NR_id", "nearest_NR_id"),
    ("nearest_NR_distance", "nearest_NR_distance"),
    ("nearest_LTE_id", "nearest_LTE_id"),
    ("nearest_LTE_distance", "nearest_LTE_distance"),
    ("nearest_UMTS_id", "nearest_UMTS_id"),
    ("nearest_UMTS_distance", "nearest_UMTS_distance"),
    ("nearest_GSM_id", "nearest_GSM_id"),
    ("nearest_GSM_distance", "nearest_GSM_distance"),
    ("pop_within_1km", "pop_within_1km"),
    ("pop_within_2km", "pop_within_2km"),
    ("pop_within_3km", "pop_within_3km"),
    ("pop_within_10km", "pop_within_10km"),
}

CONFIG_COV_COLUMN_MERGE_LOGIC = ["cellular_coverage_availability", "cellular_coverage_type"]

CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_GEOLOCATION =[
    [
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
]

CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_MASTER =[
    [
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
    # "admin1_id_giga",
    "admin2",
    # "admin2_id_giga",
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
]


#notes 
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



   