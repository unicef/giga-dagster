CREATE TABLE IF NOT EXISTS `${schema_name}`.`${table_name}` (
    school_id_giga STRING NOT NULL,
    cellular_coverage_availability STRING NOT NULL,
    cellular_coverage_type STRING NOT NULL,
    fiber_node_distance DOUBLE,
    microwave_node_distance DOUBLE,
    schools_within_1km INT,
    schools_within_2km INT,
    schools_within_3km INT,
    nearest_LTE_distance DOUBLE,
    nearest_UMTS_distance DOUBLE,
    nearest_GSM_distance DOUBLE,
    pop_within_1km LONG,
    pop_within_2km LONG,
    pop_within_3km LONG,
    pop_within_10km LONG,
    nearest_school_distance DOUBLE,
    schools_within_10km INT,
    nearest_LTE_id STRING,
    nearest_UMTS_id STRING,
    nearest_GSM_id STRING
)
USING DELTA
LOCATION '${location}';
