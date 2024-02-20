CREATE TABLE IF NOT EXISTS `${schema_name}`.`${table_name}` (
    school_id_giga STRING NOT NULL,
    pop_within_10km LONG,
    nearest_school_distance DOUBLE,
    schools_within_10km INT,
    nearest_LTE_id STRING,
    nearest_UMTS_id STRING,
    nearest_GSM_id STRING,
    education_level_govt STRING NOT NULL,
    download_speed_govt DOUBLE,
    school_id_gov_type STRING NOT NULL,
    school_address STRING,
    is_school_open STRING,
)
USING DELTA
LOCATION '${location}'
