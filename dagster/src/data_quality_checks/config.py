from src.spark.config_expectations import config

CONFIG_UNIQUE_COLUMNS = {
    "master": config.CONFIG_UNIQUE_COLUMNS_MASTER,
    "reference": config.CONFIG_UNIQUE_COLUMNS_REFERENCE,
    "geolocation": config.CONFIG_UNIQUE_COLUMNS_GEOLOCATION,
    "coverage": config.CONFIG_UNIQUE_COLUMNS_COVERAGE,
    "coverage_fb": config.CONFIG_UNIQUE_COLUMNS_COVERAGE_FB,
    "coverage_itu": config.CONFIG_UNIQUE_COLUMNS_COVERAGE_ITU,
}
CONFIG_NONEMPTY_COLUMNS = {
    "master": config.CONFIG_NONEMPTY_COLUMNS_MASTER,
    "reference": config.CONFIG_NONEMPTY_COLUMNS_REFERENCE,
    "geolocation": config.CONFIG_NONEMPTY_COLUMNS_GEOLOCATION,
    "coverage": config.CONFIG_NONEMPTY_COLUMNS_COVERAGE,
    "coverage_fb": config.CONFIG_NONEMPTY_COLUMNS_COVERAGE_FB,
    "coverage_itu": config.CONFIG_NONEMPTY_COLUMNS_COVERAGE_ITU,
}
CONFIG_VALUES_DOMAIN = {
    "master": config.CONFIG_VALUES_DOMAIN_MASTER,
    "reference": config.CONFIG_VALUES_DOMAIN_REFERENCE,
    "geolocation": config.CONFIG_VALUES_DOMAIN_GEOLOCATION,
    "coverage": config.CONFIG_VALUES_DOMAIN_COVERAGE,
}
CONFIG_VALUES_RANGE = {
    "master": config.CONFIG_VALUES_RANGE_MASTER,
    "reference": config.CONFIG_VALUES_RANGE_REFERENCE,
    "geolocation": config.CONFIG_VALUES_RANGE_GEOLOCATION,
    "coverage": config.CONFIG_VALUES_RANGE_COVERAGE,
    "coverage_itu": config.CONFIG_VALUES_RANGE_COVERAGE_ITU,
}
CONFIG_COLUMNS_EXCEPT_SCHOOL_ID = {
    "master": config.CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_MASTER,
    "geolocation": config.CONFIG_COLUMNS_EXCEPT_SCHOOL_ID_GEOLOCATION,
}
