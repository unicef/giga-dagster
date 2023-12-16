CREATE TABLE IF NOT EXISTS `${schema_name}`.`${table_name}` (
    school_id_giga STRING NOT NULL,
    download_speed_govt1 FLOAT,
    download_speed_govt5 FLOAT,
    pop_within_10km INT,
    school_id_gov_type STRING,
    education_level_govt STRING,
    education_level_regional STRING,
    school_address STRING,
    nearest_school_distance FLOAT,
    schools_within_10km INT,
    nearest_LTE_id STRING,
    nearest_UMTS_id STRING,
    nearest_GSM_id STRING,
    is_school_open STRING
)
USING DELTA
LOCATION '${location}'
