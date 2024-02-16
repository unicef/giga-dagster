import datetime

SIMILARITY_RATIO_CUTOFF = 0.7

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
    "school_id_govt_type"
]

CONFIG_NONEMPTY_COLUMNS_COVERAGE = [
    "school_id_giga",
]

CONFIG_NONEMPTY_COLUMNS_CRITICAL = [
    "school_name",
    "longitude",
    "latitude",
]


CONFIG_VALUES_DOMAIN_MASTER = {
    "cellular_coverage_availability": ["yes", "no", None],
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


    # "cellular_network_availability": ["yes", "no"],
    # "2G_coverage": ["true", "false"],
    # "3G_coverage": ["true", "false"],
    # "4G_coverage": ["true", "false"],
    # # "education_level_isced": [
    #     "childhood education",
    #     "primary education",
    #     "secondary education",
    #     "post secondary education",
    # ],
    # "school_type_public": ["public", "not public"],
    
}

CONFIG_VALUES_DOMAIN_COVERAGE = {
    "cellular_coverage_availability": ["yes", "no", None],
    "cellular_coverage_type": ["2G", "3G", "4G", "5G", "no coverage"],
}


# For COVERAGE ITU dataset. To separate COVERAGE ITU expectations into different file.
CONFIG_VALUES_RANGE_COVERAGE = {
    "fiber_node_distance": {"min": 0, "max": 10000000},
    "microwave_node_distance": {"min": 0, "max": 10000000},
    "schools_within_1km": {"min": 0, "max": 20},
    "schools_within_2km": {"min": 0, "max": 40},
    "schools_within_3km": {"min": 0, "max": 60},
    "nearest_school_distance": {"min": 0, "max": None},
    "schools_within_10km": {"min": 0, "max": 100},
}

CONFIG_VALUES_RANGE_REFERENCE = {
    "nearest_school_distance": {"min": 0, "max": None},
    "schools_within_10km": {"min": 0, "max": 100},
    "download_speed_govt": {"min": 1, "max": 200},
}

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
    "num_classroom": {"min": 0, "max": 200},
    "num_latrines": {"min": 0, "max": 200},
    "school_data_collection_year": {"min": 1000, "max": current_year},
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
    "num_classroom": {"min": 0, "max": 200},
    "num_latrines": {"min": 0, "max": 200},
    "school_data_collection_year": {"min": 1000, "max": current_year},
    "download_speed_govt": {"min": 1, "max": 200},
}

CONFIG_VALUES_RANGE_CRITICAL = {
    "latitude": {"min": -90, "max": 90},
    "longitude": {"min": -180, "max": 180},
}


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

# Coverage Column Configs

# Lower Columns
CONFIG_ITU_COLUMNS_TO_RENAME = ["Schools_within_1km", "Schools_within_2km", "Schools_within_3km", "Schools_within_10km"]

# Columns To Keep From Dataset
CONFIG_FB_COLUMNS = ["school_id_giga", "2G_coverage", "3G_coverage", "4G_coverage"]
CONFIG_ITU_COLUMNS = ["school_id_giga", "2G_coverage", "3G_coverage", "4G_coverage", 
                      "fiber_node_distance", "microwave_node_distance", "nearest_school_distance", "schools_within_1km", "schools_within_2km",
                        "schools_within_3km", "schools_within_10km", "nearest_LTE_id", "nearest_LTE_distance",
                        "nearest_UMTS_id", "nearest_UMTS_distance", "nearest_GSM_id", "nearest_GSM_distance",
                        "pop_within_1km", "pop_within_2km",
                        "pop_within_3km", "pop_within_10km"]
CONFIG_COV_COLUMNS = ["school_id_giga", "cellular_coverage_availability", "cellular_coverage_type",
                      "fiber_node_distance", "microwave_node_distance", "nearest_school_distance", "schools_within_1km", "schools_within_2km",
                        "schools_within_3km", "schools_within_10km", "nearest_LTE_id", "nearest_LTE_distance",
                        "nearest_UMTS_id", "nearest_UMTS_distance", "nearest_GSM_id", "nearest_GSM_distance",
                        "pop_within_1km", "pop_within_2km",
                        "pop_within_3km", "pop_within_10km"]

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
    "num_classroom",
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
    "num_classroom",
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