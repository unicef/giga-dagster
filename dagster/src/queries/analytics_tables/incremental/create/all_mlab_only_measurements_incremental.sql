CREATE TABLE delta_lake.default.all_mlab_only_measurements_incremental
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_mlab_only_measurements_incremental'
)
AS


-- =============================================================================
-- CTE: school_lookup
-- Purpose: Creates a reusable lookup for school metadata with pre-computed timezone
--
-- IDENTICAL to GigaMeter script school_lookup CTE
-- Reused pattern ensures consistency across both source scripts
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
        -- PRE-COMPUTED: Timezone with fallback
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
        -- FILTER: Exclude deleted schools at source
        school.deleted IS NULL
       -- DEBUGGING: Uncomment to test specific school
       -- and school.external_id = 'bw10ps006'
),


-- =============================================================================
-- CTE: measurements_base
-- Purpose: Extracts raw MLAB measurement data with minimal transformation
--
-- DIFFERENCE from GigaMeter:
--   - Uses school_id field directly (government ID, not giga_id)
--   - Extracts client_country from JSON for school matching
--   - Filter: source = 'MLab'
-- =============================================================================
measurements_base AS (
    SELECT
        id AS measurement_id,
        uuid,
        giga_id_school AS school_id_giga,
        -- MLAB uses school_id (government ID) directly from measurement
        school_id as school_id_govt,
        created_at AS created_timestamp,
        CAST(DATE_TRUNC('day', created_at) AS date) AS date,
        -- Raw values - conversion done later
        download,
        upload,
        latency,
        data_downloaded,
        data_uploaded,
        data_usage,
        results,
        client_info,
        -- PRE-EXTRACT: Country code for school matching
        -- Used in JOIN condition for school_lookup
        json_extract_scalar(client_info, '$.Country') AS client_country,
        server_info,
        wifi_connections,
        ip_address,
        app_version,
        notes,
        browser_id,
        device_hardware_id AS device_id,
        installed_path,
        windows_username,
        -- Geolocation fields (available from app version 2.0.3+)
        detected_latitude,
        detected_longitude,
        detected_location_is_flagged,
        detected_location_distance,
        detected_location_accuracy
    FROM
        gigameter_production_db.public.measurements
    WHERE
        source = 'MLab'  -- MLAB measurements only
    ORDER BY id
    LIMIT 40000
)


-- =============================================================================
-- CTE: measurements_enriched
-- Purpose: Joins measurements with school lookup for enrichment
--
-- CRITICAL DIFFERENCE from GigaMeter:
--   JOIN uses client_country + school_id_govt (not school_id_giga)
--   This is because MLAB measurements don't have reliable giga_id_school
--
-- Original comment preserved:
--   "MLAB joins on school_id_govt + country, not school_id_giga"
-- =============================================================================
, measurements_enriched as (


SELECT
    m.measurement_id,
    m.uuid,
    -- NOTE: school_id_giga comes from school_lookup, not measurement
    -- MLAB measurements may have NULL or unreliable giga_id_school
    s.school_id_giga,
    s.school_name,
    s.school_id_govt,
    s.country,
    s.iso3_code,
    CAST(m.created_timestamp AS timestamp with time zone) as created_timestamp,
    m.date,
    -- TIMEZONE CONVERSION: Done here using pre-computed timezone
    CAST(at_timezone(m.created_timestamp, s.timezone) AS timestamp) AS local_created_timestamp,
    m.download,
    m.upload,
    m.latency,
    m.data_downloaded,
    m.data_uploaded,
    m.data_usage,
    m.results,
    m.client_info,
    m.client_country,
    m.server_info,
    m.ip_address,
    m.wifi_connections,
    m.app_version,
    m.notes,
    m.browser_id,
    m.device_id,
    m.installed_path,
    m.windows_username,
    m.detected_latitude,
    m.detected_longitude,
    m.detected_location_is_flagged,
    m.detected_location_distance,
    m.detected_location_accuracy
FROM
    measurements_base m
LEFT JOIN school_lookup s
    -- MLAB JOIN CONDITION:
    -- 1. Match country code from client_info to school's country
    -- 2. Match school_id (government ID) from measurement to school's external_id
    -- CAVEAT: If either doesn't match, school fields will be NULL
    ON TRIM(m.client_country) = TRIM(s.iso2_code)
    AND m.school_id_govt = s.school_id_govt


)




-- =============================================================================
-- FINAL SELECT
-- Purpose: Apply unit conversions, extract JSON fields, compute time analysis
--
-- IDENTICAL STRUCTURE to GigaMeter script with two exceptions:
--   1. app_version uses COALESCE to default to 'mlab' when NULL
--   2. rt_source is 'Mlab' instead of 'GigaMeter'
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
    -- Speed Metrics (kbps -> Mbps)
    -- -------------------------------------------------------------------------
    ROUND(CAST((download / 1000) AS REAL), 2)                                                       AS download_speed,
   -- ROUND(CAST((upload / 1000) AS REAL), 2)                                                         AS upload_speed,
    CAST(latency AS BIGINT)                                                                          AS latency,

    -- -------------------------------------------------------------------------
    -- Data Volume Metrics (bytes -> GB)
    -- -------------------------------------------------------------------------
    (ROUND(CAST((data_downloaded / 1000) AS real), 2) / 1048576) AS data_downloaded_gb,
    (ROUND(CAST((data_uploaded / 1000) AS real), 2) / 1048576) AS data_uploaded_gb,
    (ROUND(CAST((data_usage / 1000) AS real), 2) / 1048576) AS data_usage_gb,

    -- -------------------------------------------------------------------------
    -- Server Detection (simplified from approx_most_frequent)
    -- -------------------------------------------------------------------------
    CAST(JSON_EXTRACT(server_info, '$.City') AS VARCHAR) AS server_location,

    -- -------------------------------------------------------------------------
    -- ISP/Client Information
    -- -------------------------------------------------------------------------
    -- UPDATED: 2026-05-12 BY LUKE STRINGER - correction for alternative client.info json format
    --CAST(JSON_EXTRACT(client_info, '$.ISP') AS VARCHAR)                                             AS detected_isp_raw,
    COALESCE(CAST(json_extract_scalar(client_info, '$.ASN.name') AS VARCHAR), CAST(JSON_EXTRACT(client_info, '$.ISP') AS VARCHAR)) AS detected_isp_raw,
    --CAST(json_extract_scalar(client_info, '$.ASN') AS VARCHAR)                                      AS detected_isp_asn,
    COALESCE(CAST(json_extract_scalar(client_info, '$.ASN.asn') AS VARCHAR),  CAST(json_extract_scalar(client_info, '$.ASN') AS VARCHAR))   AS detected_isp_asn,


    CAST(JSON_EXTRACT(client_info, '$.IP') AS VARCHAR) AS detected_isp_ip_address,
    CAST(client_country AS VARCHAR) AS client_country,
    CAST(ip_address AS VARCHAR) AS ip_address,

    -- MLAB-SPECIFIC: Default app_version to 'mlab' when NULL
    -- Original hardcoded 'mlab' AS app_version; this preserves actual version if present
    CAST(COALESCE(app_version, 'mlab') AS VARCHAR) as app_version,
    CAST('Mlab' AS VARCHAR) as rt_source,                 -- MLAB source identifier
    CAST(notes AS VARCHAR) as notes,

    -- -------------------------------------------------------------------------
    -- WiFi Connection Details
    -- -------------------------------------------------------------------------
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].ssid') AS VARCHAR) AS detected_wifi_ssid,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].model') AS VARCHAR) AS detected_wifi_model,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].quality') AS INTEGER) AS detected_wifi_quality,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].signalLevel') AS DECIMAL(10,2)) AS detected_wifi_signal,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].txRate') AS INTEGER) AS detected_wifi_tx_rate,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].channel') AS INTEGER) AS detected_wifi_channel,
    CAST(json_extract_scalar(CAST(wifi_connections AS JSON), '$[0].frequency') AS INTEGER) AS detected_wifi_frequency,


    -- -------------------------------------------------------------------------
    -- Device Identification
    -- -------------------------------------------------------------------------
    CAST(browser_id AS VARCHAR) AS browser_id,
    CAST(device_id AS VARCHAR) AS device_id,


    -- -------------------------------------------------------------------------
    -- Installation
    -- -------------------------------------------------------------------------
    CAST(installed_path AS VARCHAR) AS installed_path,
    CAST(windows_username AS VARCHAR) AS windows_username,

    -- -------------------------------------------------------------------------
    -- Geolocation (available from app version 2.0.3+)
    -- -------------------------------------------------------------------------
    CAST(detected_latitude AS DOUBLE)                                                                AS detected_latitude,
    CAST(detected_longitude AS DOUBLE)                                                               AS detected_longitude,
    CAST(detected_location_is_flagged AS BOOLEAN)                                                    AS detected_location_is_flagged,
    CAST(detected_location_distance AS DOUBLE)                                                       AS detected_location_distance,
    CAST(detected_location_accuracy AS DOUBLE)                                                       AS detected_location_accuracy,


-- -------------------------------------------------------------------------
          -- Packet Loss Indicators (S2C - Server to Client)
    -- Extracted from results JSON: NDTResult.S2C.LastServerMeasurement.TCPInfo
    -- -------------------------------------------------------------------------
    CAST(json_extract_scalar(CAST(results AS JSON), '$["NDTResult.S2C"].LastServerMeasurement.TCPInfo.Lost') AS INTEGER)         AS s2c_lost,
    CAST(json_extract_scalar(CAST(results AS JSON), '$["NDTResult.S2C"].LastServerMeasurement.TCPInfo.BytesRetrans') AS DOUBLE)  AS s2c_bytes_retrans,
    CAST(json_extract_scalar(CAST(results AS JSON), '$["NDTResult.S2C"].LastServerMeasurement.TCPInfo.BytesSent') AS DOUBLE)     AS s2c_bytes_sent,
   ---------------
    -- elapsed time --
    ---------------
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C"].LastClientMeasurement.ElapsedTime') AS VARCHAR) AS s2c_lastclient_elapsed_time,  -- download  time
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.C2S"].LastClientMeasurement.ElapsedTime') AS VARCHAR) AS c2s_lastclient_elapsed_time,  -- upload time
    ---------------
    -- DATA SIZE --
    ---------------
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.S2C"].LastServerMeasurement.TCPInfo.BytesAcked') AS VARCHAR) AS s2c_bytes_Acked,       -- download size
    CAST(JSON_EXTRACT_SCALAR(results, '$["NDTResult.C2S"].LastServerMeasurement.TCPInfo.BytesAcked') AS VARCHAR) AS c2s_bytes_Acked,       -- upload size
    -------
    -- json
    -------
    CAST(JSON_FORMAT(CAST(JSON_EXTRACT(results, '$["NDTResult.S2C"].LastServerMeasurement') AS JSON)) AS VARCHAR) AS s2c_FinalSnapshot,   --  download json populated
    CAST(JSON_FORMAT(CAST(JSON_EXTRACT(results, '$["NDTResult.C2S"].LastServerMeasurement') AS JSON)) AS VARCHAR) AS c2s_FinalSnapshot,   --  upload json populated

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


;

-- =============================================================================
-- MLAB-SPECIFIC NOTES
--
-- 1. SCHOOL MATCHING RELIABILITY
--    - MLAB uses client_info.Country + school_id (govt ID) for matching
--    - Less reliable than GigaMeter which uses giga_id_school directly
--    - Unmatched measurements will have NULL school metadata
--    - Consider data quality checks on match rate
--
-- 2. APP VERSION
--    - MLAB measurements often have NULL app_version
--    - COALESCE defaults to 'mlab' for identification
--    - Original script hardcoded 'mlab' AS app_version
--
-- 3. DOWNSTREAM JOIN
--    - Script 3 joins MLAB data on school_id_govt + iso3_code
--    - This differs from GigaMeter which joins on school_id_giga
--    - Maintains original matching logic for consistency
-- =============================================================================
