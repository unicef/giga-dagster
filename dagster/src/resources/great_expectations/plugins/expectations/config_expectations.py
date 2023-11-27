import datetime

# Single column checks
CONFIG_UNIQUE_COLUMNS = ["school_id", "giga_id_school"]
CONFIG_NONEMPTY_COLUMNS = [
    "school_name",
    "longitude",
    "latitude",
    "education_level",
    "mobile_internet_generation",
    "internet_availability",
    "internet_type",
    "internet_speed_mbps",
]
CONFIG_NOT_SIMILAR_COLUMN = ["school_name"]
CONFIG_FIVE_DECIMAL_PLACES = ["latitude", "longitude"]

# Multi-column checks
CONFIG_UNIQUE_SET_COLUMNS = [
    ["school_id", "school_name", "education_level", "latitude", "longitude"],
    ["school_name", "education_level", "latitude", "longitude"],
]

CONFIG_COLUMN_SUM = [
    ["student_count_girls", "student_count_boys", "student_count_others"]
]

# Single Column w/ parameters
date_today = datetime.date.today()
current_year = date_today.year

# For GOVERNMENT SCHOOL COLUMNS
CONFIG_VALUES_RANGE = {
    # Mandatory
    "internet_speed_mbps": {"min": 1, "max": 20},
    "student_count": {"min": 20, "max": 2500},
    "latitude": {"min": -90, "max": 90},
    "longitude": {"min": -180, "max": 180},
    "school_establishment_year": {"min": 1000, "max": current_year},
    # # Calculated
    "school_density": {"min": 0, "max": 5},
    # Optional
    "connectivity_speed_static": {"min": 0, "max": 200},
    "connectivity_speed_contracted": {"min": 0, "max": 500},
    "connectivity_latency_static": {"min": 0, "max": 200},
    "num_computers": {"min": 0, "max": 500},
    "num_computers_desired": {"min": 0, "max": 1000},
    "num_teachers": {"min": 0, "max": 200},
    "num_adm_personnel": {"min": 0, "max": 200},
    "num_students": {"min": 0, "max": 10000},
    "num_classrooms": {"min": 0, "max": 200},
    "num_latrines": {"min": 0, "max": 200},
    "school_data_collection_year": {"min": 1000, "max": current_year},
}

CONFIG_VALUES_OPTIONS = {
    "computer_lab": ["yes", "no"],
    "electricity_availability": ["yes", "no"],
    "water_availability": ["yes", "no"],
    "cellular_network_availability": ["yes", "no"],
    "connectivity_availability": ["yes", "no"],
    "school_is_open": ["yes", "no"],
    "school_daily_check_app": ["yes", "no"],
    "2G_coverage": ["true", "false"],
    "3G_coverage": ["true", "false"],
    "4G_coverage": ["true", "false"],
    "education_level_isced": [
        "childhood education",
        "primary education",
        "secondary education",
        "post secondary education",
    ],
    "connectivity_type": [
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
    "school_type_public": ["public", "not public"],
    "electricity_type": [
        "electrical grid",
        "diesel generator",
        "solar power station",
        "other",
    ],
    "school_data_collection_modality": ["online", "in-person", "phone", "other"],
    "cellular_network_type": ["2g", "3g", "4g", "5g"],
}

# For COVERAGE ITU dataset. To separate COVERAGE ITU expectations into different file.
CONFIG_VALUES_RANGE_COVERAGE_ITU = {
    "fiber_node_distance": {"min": 0, "max": None},
    "microwave_node_distance": {"min": 0, "max": None},
    "nearest_school_distance": {"min": 0, "max": None},
    "schools_within_1km": {"min": 0, "max": 20},
    "schools_within_2km": {"min": 0, "max": 40},
    "schools_within_3km": {"min": 0, "max": 60},
    "schools_within_10km": {"min": 0, "max": 100},
}

CONFIG_VALUES_TYPE = [{"column": "school_id", "type": "int64"}]

# Column Pairs
CONFIG_PAIR_AVAILABILITY = [
    {"availability_column": "internet_availability", "value_column": "internet_type"},
    {
        "availability_column": "internet_availability",
        "value_column": "internet_speed_mbps",
    },
]
