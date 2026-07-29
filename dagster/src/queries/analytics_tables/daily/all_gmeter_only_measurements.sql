



  CREATE TABLE IF NOT EXISTS default.all_gmeter_only_measurements as (


-- =============================================================================
-- CTE: school_lookup
-- Purpose: Creates a reusable lookup for school metadata with pre-computed timezone
--
-- Timezone Fallback Chain:
--   1. Try timezone by ISO3 code (tz.timezone)
--   2. Try timezone by country name (tz_name.timezone)
--   3. Default to 'UTC'
-- =============================================================================
WITH school_lookup AS (
    SELECT
        school.giga_id_school AS school_id_giga,
        school.external_id AS school_id_govt,
        school.name AS school_name,
        country.name AS country,
        country.code AS iso2_code,
        -- RENAMED: iso3_format -> iso3_code for clarity
        UPPER(TRIM(country.iso3_format)) AS iso3_code,
        -- PRE-COMPUTED: Timezone with fallback, eliminates JOINs in final SELECT
        COALESCE(tz.timezone, tz_name.timezone, 'UTC') AS timezone
    FROM
        gigameter_production_db.public.school
    LEFT JOIN
        gigameter_production_db.public.country
        ON country.id = school.country_id
    -- Timezone lookup by ISO3 code (primary method)
    LEFT JOIN default.country_timezones tz
        ON country.iso3_format = tz.iso3
    -- Timezone lookup by country name (fallback method)
    LEFT JOIN default.country_timezones tz_name
        ON LOWER(country.name) = LOWER(tz_name.country)
    WHERE
        -- FILTER: Exclude deleted schools at source (was in final WHERE clause)
        school.deleted IS NULL
),


-- =============================================================================
-- CTE: measurements_base
-- Purpose: Extracts raw measurement data with minimal transformation
--
-- Filter: source = 'DailyCheckApp' (GigaMeter mobile app only)
-- =============================================================================
measurements_base AS (
    SELECT
        id AS measurement_id,
        uuid,
        giga_id_school AS school_id_giga,
        created_at AS created_timestamp,
        -- Date truncated to day for partitioning/filtering
        CAST(DATE_TRUNC('day', created_at) AS date) AS date,
        -- Raw values - conversion done later
        download,
        upload,
        latency,
        data_downloaded,
        data_uploaded,
        data_usage,
        -- JSON fields passed through for extraction in final SELECT
        results,
        client_info,
        server_info,
        wifi_connections,
        ip_address,
        app_version,
        notes,
        browser_id,
        device_hardware_id AS device_id,
        installed_path,
        windows_username
    FROM
        gigameter_production_db.public.measurements
    WHERE
        source = 'DailyCheckApp'  -- GigaMeter app measurements only

)


-- =============================================================================
-- CTE: measurements_enriched
-- Purpose: Joins measurements with school lookup for enrichment
-- =============================================================================
, measurements_enriched as (
  SELECT
      m.measurement_id,
      m.uuid,
      m.school_id_giga,
      s.school_name,
      s.school_id_govt,
      s.country,
      s.iso3_code,
      -- Preserve timezone for timestamp
      CAST(m.created_timestamp AS timestamp with time zone) as created_timestamp,
      m.date,
      -- TIMEZONE CONVERSION: Done here using pre-computed timezone from school_lookup
      -- Original did this in final SELECT on combined dataset
      CAST(at_timezone(m.created_timestamp, s.timezone) AS timestamp) AS local_created_timestamp,
      -- Pass through raw values for final transformation
      m.download,
      m.upload,
      m.latency,
      m.data_downloaded,
      m.data_uploaded,
      m.data_usage,
      m.results,
      m.client_info,
      m.server_info,
      m.ip_address,
      m.wifi_connections,
      m.app_version,
      m.notes,
      m.browser_id,
      m.device_id,
      m.installed_path,
      m.windows_username
  FROM
      measurements_base m
  LEFT JOIN
      school_lookup s
      -- GigaMeter uses giga_id_school for school matching
      ON m.school_id_giga = s.school_id_giga

 -- WHERE
   --  m.date > cast('2024-01-01' as date)
)



-- =============================================================================
-- FINAL SELECT
-- Purpose: Apply unit conversions, extract JSON fields, compute time analysis
--
-- Key Transformations:
--   - Speed: kbps -> Mbps (divide by 1000)
--   - Data volumes: bytes -> GB (divide by 1000, then by 1048576)
--   - JSON extraction for server_info, client_info, wifi_connections
--   - Local time analysis (hour, day of week, school hours)
--
-- NO AGGREGATION: Each row is one measurement (unlike original with GROUP BY)
-- =============================================================================
select
    CAST(measurement_id AS BIGINT)                                                                   AS measurement_id,
    CAST(uuid AS VARCHAR)                                                                            AS measurement_uuid,
    CAST(school_id_giga AS VARCHAR)                                                                  AS school_id_giga,
    CAST(school_name AS VARCHAR)                                                                     AS school_name,
    CAST(school_id_govt AS VARCHAR)                                                                  AS school_id_govt,
    CAST(country AS VARCHAR)                                                                         AS country,
    CAST(iso3_code AS VARCHAR)                                                                       AS iso3_code,
    CAST(created_timestamp AS TIMESTAMP WITH TIME ZONE)                                              AS created_timestamp,
    CAST(date AS DATE)                                                                               AS date,
    CAST(local_created_timestamp AS TIMESTAMP)                                                       AS local_created_timestamp,

    -- -------------------------------------------------------------------------
    -- Time Analysis Fields (Local Timezone)
    -- -------------------------------------------------------------------------
    CAST(EXTRACT(HOUR FROM local_created_timestamp) AS BIGINT)                                       AS local_hour_of_measurement,
    CAST(format_datetime(local_created_timestamp, 'EEEE') AS VARCHAR)                               AS local_day_of_week,

    CAST(
        CASE
            WHEN day_of_week(local_created_timestamp) BETWEEN 1 AND 5
            THEN true
            ELSE false
        END
    AS BOOLEAN)                                                                                      AS is_weekday,

    CAST(
        CASE
            WHEN EXTRACT(HOUR FROM local_created_timestamp) BETWEEN 8 AND 16
            THEN '8am-4pm(within school hrs)'
            ELSE '5pm-7am(outside school hrs)'
        END
    AS VARCHAR)                                                                                      AS measurement_time_window,

    -- -------------------------------------------------------------------------
    -- Speed Metrics
    -- Conversion: kbps -> Mbps (divide by 1000)
    -- -------------------------------------------------------------------------
    ROUND(CAST((download / 1000) AS REAL), 2)                                                       AS download_speed,
   -- ROUND(CAST((upload / 1000) AS REAL), 2)                                                         AS upload_speed,
    CAST(latency AS BIGINT)                                                                          AS latency,

    -- -------------------------------------------------------------------------
    -- Data Volume Metrics
    -- Conversion: bytes -> KB -> GB
    -- -------------------------------------------------------------------------
    (ROUND(CAST((data_downloaded / 1000) AS REAL), 2) / 1048576)                                    AS data_downloaded_gb,
    (ROUND(CAST((data_uploaded / 1000) AS REAL), 2) / 1048576)                                      AS data_uploaded_gb,
    (ROUND(CAST((data_usage / 1000) AS REAL), 2) / 1048576)                                         AS data_usage_gb,

    -- -------------------------------------------------------------------------
    -- Server Detection
    -- -------------------------------------------------------------------------
    CAST(JSON_EXTRACT(server_info, '$.City') AS VARCHAR)                                             AS server_location,

    -- -------------------------------------------------------------------------
    -- ISP/Client Information
    -- -------------------------------------------------------------------------
    -- UPDATED: 2026-05-12 BY LUKE STRINGER - correction for alternative client.info json format
    --CAST(JSON_EXTRACT(client_info, '$.ISP') AS VARCHAR)                                             AS detected_isp_raw,
    COALESCE(CAST(json_extract_scalar(client_info, '$.ASN.name') AS VARCHAR), CAST(JSON_EXTRACT(client_info, '$.ISP') AS VARCHAR)) AS detected_isp_raw,
    --CAST(json_extract_scalar(client_info, '$.ASN') AS VARCHAR)                                      AS detected_isp_asn,
    COALESCE(CAST(json_extract_scalar(client_info, '$.ASN.asn') AS VARCHAR),  CAST(json_extract_scalar(client_info, '$.ASN') AS VARCHAR))   AS detected_isp_asn,


    CAST(JSON_EXTRACT(client_info, '$.IP') AS VARCHAR)                                              AS detected_isp_ip_address,
    CAST(json_extract_scalar(client_info, '$.Country') AS VARCHAR)                                  AS client_country,
    CAST(ip_address as VARCHAR)                                                                     AS ip_address,

    CAST(app_version AS VARCHAR)                                                                     AS app_version,
    CAST('GigaMeter' AS VARCHAR)                                                                     AS rt_source,
    CAST(notes AS VARCHAR)                                                                           AS notes,

    -- -------------------------------------------------------------------------
    -- WiFi Connection Details
    -- -------------------------------------------------------------------------
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].ssid') AS VARCHAR)               AS detected_wifi_ssid,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].model') AS VARCHAR)              AS detected_wifi_model,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].quality') AS INTEGER)            AS detected_wifi_quality,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].signalLevel') AS DECIMAL(10,2))  AS detected_wifi_signal,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].txRate') AS INTEGER)             AS detected_wifi_tx_rate,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].channel') AS INTEGER)            AS detected_wifi_channel,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].frequency') AS INTEGER)          AS detected_wifi_frequency,

    -- -------------------------------------------------------------------------
    -- Device Identification
    -- -------------------------------------------------------------------------
    CAST(browser_id AS VARCHAR)                                                                      AS browser_id,
    CAST(device_id AS VARCHAR)                                                                       AS device_id,

    -- -------------------------------------------------------------------------
    -- Installation
    -- -------------------------------------------------------------------------
    CAST(installed_path AS VARCHAR)                                                                  AS installed_path,
    CAST(windows_username AS VARCHAR)                                                                AS windows_username,

    -- -------------------------------------------------------------------------
    -- Packet Loss Indicators (S2C - Server to Client)
    -- -------------------------------------------------------------------------
    CAST(json_extract_scalar(CAST(results AS JSON), '$["NDTResult.S2C"].LastServerMeasurement.TCPInfo.Lost') AS INTEGER)         AS s2c_lost,
    CAST(json_extract_scalar(CAST(results AS JSON), '$["NDTResult.S2C"].LastServerMeasurement.TCPInfo.BytesRetrans') AS DOUBLE)  AS s2c_bytes_retrans,
    CAST(json_extract_scalar(CAST(results AS JSON), '$["NDTResult.S2C"].LastServerMeasurement.TCPInfo.BytesSent') AS DOUBLE)     AS s2c_bytes_sent,

    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C"].LastClientMeasurement.ElapsedTime') AS VARCHAR)                     AS s2c_lastclient_elapsed_time,
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.C2S"].LastClientMeasurement.ElapsedTime') AS VARCHAR)                     AS c2s_lastclient_elapsed_time,

    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C"].LastServerMeasurement.TCPInfo.BytesAcked') AS VARCHAR)              AS s2c_bytes_Acked,
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.C2S"].LastServerMeasurement.TCPInfo.BytesAcked') AS VARCHAR)              AS c2s_bytes_Acked,

    CAST(JSON_FORMAT(CAST(JSON_EXTRACT(results, '$["NDTResult.S2C"].LastServerMeasurement') AS JSON)) AS VARCHAR)             AS s2c_FinalSnapshot,
    CAST(JSON_FORMAT(CAST(JSON_EXTRACT(results, '$["NDTResult.C2S"].LastServerMeasurement') AS JSON)) AS VARCHAR)             AS c2s_FinalSnapshot,

    -- -------------------------------------------------------------------------
    -- NDT-7 Upload (C2S) TCP-based duration/bytes
    -- Used for Scenario C_mix: TCP-based upload speed + duration validation
    -- -------------------------------------------------------------------------
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.C2S"].LastServerMeasurement.TCPInfo.ElapsedTime') AS BIGINT)              AS c2s_tcpinfo_elapsed_time_us,  -- microseconds
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.C2S"].LastServerMeasurement.TCPInfo.BytesReceived') AS BIGINT)            AS c2s_bytes_received,

    -- Legacy upload speed (client-reported, kbps -> Mbps), kept for comparison
    ROUND(CAST((upload / 1000) AS REAL), 2)                                                                                    AS upload_speed_legacy,
    -- Protocol-aware upload speed:
    --   NDT-7: TCP-based (BytesReceived * 8 / ElapsedTime, both in NDTResult.C2S.LastServerMeasurement.TCPInfo)
    --   NDT-5: c2sRate / 1000 (Roberto's documented formula; equals upload/1000 i.e. upload_speed_legacy)
    CASE
        WHEN JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C"].LastClientMeasurement.ElapsedTime') IS NOT NULL
            THEN ROUND(CAST((CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.C2S"].LastServerMeasurement.TCPInfo.BytesReceived') AS DOUBLE) * 8
                / NULLIF(CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.C2S"].LastServerMeasurement.TCPInfo.ElapsedTime') AS DOUBLE), 0)) AS REAL), 2)
        WHEN JSON_EXTRACT_SCALAR(results, '$.c2sRate') IS NOT NULL
            THEN ROUND(CAST((CAST(JSON_EXTRACT_SCALAR(results, '$.c2sRate') AS DOUBLE) / 1000) AS REAL), 2)
        ELSE NULL
    END AS upload_speed,

    -- -------------------------------------------------------------------------
    -- NDT-5 flat fields
    -- NDT-5 results are a flat JSON object (no nested NDTResult.S2C/C2S structure)
    -- -------------------------------------------------------------------------
    CAST(JSON_EXTRACT_SCALAR(results, '$.c2sRate') AS DOUBLE)                                                                  AS ndt5_c2s_rate_kbps,
    CAST(JSON_EXTRACT_SCALAR(results, '$["TCPInfo.BytesAcked"]') AS BIGINT)                                                        AS ndt5_bytes_acked,
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C.StartTime"]') AS VARCHAR)                                                   AS ndt5_s2c_start_str,
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C.EndTime"]') AS VARCHAR)                                                     AS ndt5_s2c_end_str,

    -- NDT-5 duration: difference of embedded Go monotonic clock readings (m=+...)
    -- in start/end timestamp strings, e.g. '... +0000 UTC m=+1763116.774629046'.
    -- Avoids unreliable wall-clock date_parse (9-digit nanosecond fractions).
    (CAST(regexp_extract(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C.EndTime"]'), 'm=\+([0-9.]+)', 1) AS DOUBLE)
        - CAST(regexp_extract(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C.StartTime"]'), 'm=\+([0-9.]+)', 1) AS DOUBLE))        AS ndt5_duration_s,

    -- NDT-5 alternative download speed (Roberto's TCP-based formula) - NOT used in
    -- download_speed or validity flags by default. NULL unless TCPInfoBytesAcked and
    -- a parseable duration are both present (~19.3% of NDT-5 tests). For monitoring /
    -- future switch-over only.
    ROUND(CAST(8 * CAST(JSON_EXTRACT_SCALAR(results, '$["TCPInfo.BytesAcked"]') AS DOUBLE)
        / NULLIF((CAST(regexp_extract(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C.EndTime"]'), 'm=\+([0-9.]+)', 1) AS DOUBLE)
            - CAST(regexp_extract(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C.StartTime"]'), 'm=\+([0-9.]+)', 1) AS DOUBLE)), 0)
        / 1e6 AS REAL), 2)                                                                                                     AS ndt5_download_speed_alt

FROM
  measurements_enriched

   )
