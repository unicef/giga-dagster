from src.spark.config_expectations import config

CONFIG_UNIQUE_COLUMNS = {
    "master": config.UNIQUE_COLUMNS_MASTER,
    "reference": config.UNIQUE_COLUMNS_REFERENCE,
    "geolocation": config.UNIQUE_COLUMNS_GEOLOCATION,
    "coverage": config.UNIQUE_COLUMNS_COVERAGE,
    "coverage_fb": config.UNIQUE_COLUMNS_COVERAGE_FB,
    "coverage_itu": config.UNIQUE_COLUMNS_COVERAGE_ITU,
    "qos": config.UNIQUE_COLUMNS_QOS,
}
CONFIG_NONEMPTY_COLUMNS = {
    "master": config.NONEMPTY_COLUMNS_MASTER,
    "reference": config.NONEMPTY_COLUMNS_REFERENCE,
    "geolocation": config.NONEMPTY_COLUMNS_GEOLOCATION,
    "coverage": config.NONEMPTY_COLUMNS_COVERAGE,
    "coverage_fb": config.NONEMPTY_COLUMNS_COVERAGE_FB,
    "coverage_itu": config.NONEMPTY_COLUMNS_COVERAGE_ITU,
    "qos": config.NONEMPTY_COLUMNS_QOS,
}
CONFIG_VALUES_DOMAIN = {
    "master": config.VALUES_DOMAIN_MASTER,
    "reference": config.VALUES_DOMAIN_REFERENCE,
    "geolocation": config.VALUES_DOMAIN_GEOLOCATION,
    "coverage": config.VALUES_DOMAIN_COVERAGE,
    "coverage_itu": config.VALUES_DOMAIN_COVERAGE_ITU,
}
CONFIG_VALUES_RANGE = {
    "master": config.VALUES_RANGE_MASTER,
    "reference": config.VALUES_RANGE_REFERENCE,
    "geolocation": config.VALUES_RANGE_GEOLOCATION,
    "coverage": config.VALUES_RANGE_COVERAGE,
    "coverage_itu": config.VALUES_RANGE_COVERAGE_ITU,
}
CONFIG_COLUMNS_EXCEPT_SCHOOL_ID = {
    "master": config.COLUMNS_EXCEPT_SCHOOL_ID_MASTER,
    "geolocation": config.COLUMNS_EXCEPT_SCHOOL_ID_GEOLOCATION,
}
