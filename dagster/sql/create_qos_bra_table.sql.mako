CREATE OR REPLACE TABLE `${schema_name}`.`${table_name}` (
    id STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    country_id STRING NOT NULL,
    school_id_gov STRING NOT NULL,
    school_id_giga STRING NOT NULL,
    speed_download FLOAT,
    speed_upload FLOAT,
    roundtrip_time FLOAT,
    jitter_download FLOAT,
    jitter_upload FLOAT,
    rtt_packet_loss_pct FLOAT,
    latency FLOAT,
    provider STRING,
    ip_family INT,
    report_id STRING,
    agent_id STRING
)
USING DELTA
LOCATION '${location}'
PARTITIONED BY (date)
