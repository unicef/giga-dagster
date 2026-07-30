
CREATE TABLE  delta_lake.test_tables.all_gigameter_measurement_data_incremental

WITH (
    location = 'abfss://giga-dataops-prod@saunigiga.dfs.core.windows.net/warehouse/all_gigameter_measurement_data_incremental'
)
AS

--STEP 4a

-- =============================================================================
-- CTE: measure
-- Purpose: Combines GigaMeter and MLAB measurements with school master enrichment
--
-- STRUCTURE: Two SELECT statements with UNION ALL
--   1. GigaMeter: Joins on school_id_giga
--   2. MLAB: Joins on school_id_govt + iso3_code (country required for match)
--
-- SCHOOL MASTER FIELDS ADDED:
--   - admin1, admin2 (administrative regions)
--   - connectivity, connectivity_type_govt
--   - cellular_coverage_type
--   - education_level
--   - electricity_availability
--   - fiber_node_distance
--   - school_area_type, school_funding_type
--   - latitude, longitude
--
-- NOTE: 'deleted' column commented out - filtering done in upstream scripts
-- =============================================================================
WITH  measure AS (

    -- =========================================================================
    -- PART 1: GigaMeter Measurements
    -- Source: delta_lake.test_tables.all_gmeter_only_measurements_incremental
    -- Join: school_id_giga (reliable, from GigaMeter app registration)
    -- =========================================================================
    SELECT
      m1.*,                              -- All columns from upstream table
      -- School master enrichment fields
      s1.admin1,
      s1.admin2,
      s1.connectivity,
      s1.connectivity_type_govt,
      s1.cellular_coverage_type,
      s1.education_level,
      s1.electricity_availability,
      s1.fiber_node_distance,
      s1.school_area_type,
      s1.school_funding_type,
      --s1.deleted,                      -- REMOVED: Filtered upstream
      s1.latitude,
      s1.longitude
    FROM
      delta_lake.test_tables.all_gmeter_only_measurements_incremental m1
    LEFT JOIN
      test_tables.all_school_master s1
    on
      -- GigaMeter uses reliable giga_id for matching
      m1.school_id_giga = s1.school_id_giga

      -- Duplicate protection


UNION ALL


    -- =========================================================================
    -- PART 2: MLAB Measurements
    -- Source: delta_lake.test_tables.all_mlab_only_measurements_incremental
    -- Join: school_id_govt + iso3_code (requires country for disambiguation)
    --
    -- CAVEAT: MLAB matching is less reliable
    --   - Uses government school ID which may not be unique globally
    --   - Requires country (iso3_code) to disambiguate
    --   - Unmatched rows will have NULL school master fields
    -- =========================================================================
    SELECT
      m2.*,
      s2.admin1,
      s2.admin2,
      s2.connectivity,
      s2.connectivity_type_govt,
      s2.cellular_coverage_type,
      s2.education_level,
      s2.electricity_availability,
      s2.fiber_node_distance,
      s2.school_area_type,
      s2.school_funding_type,
     -- s2.deleted,                       -- REMOVED: Filtered upstream
      s2.latitude,
      s2.longitude
    FROM
      delta_lake.test_tables.all_mlab_only_measurements_incremental m2
    LEFT JOIN
      test_tables.all_school_master s2
    on
      -- MLAB requires compound key for reliable matching
      m2.school_id_govt = s2.school_id_govt
    and
      m2.iso3_code = s2.iso3_code


)


-- =============================================================================
-- FINAL SELECT
-- Purpose: Final column selection with:
--   - ISP name cleaning
--   - Backwards compatibility aliases
--   - Country-specific enrichments (Bosnia, connectivity credits)
--
-- IMPROVEMENTS:
--   - Timezone fields already computed (just pass through)
--   - No need for timezone JOINs here
--   - Cleaner, more readable structure
-- =============================================================================


SELECT
 -- DISTINCT                               -- Deduplication (was via GROUP BY in original).  -- count with/without distinct 5,668,591 vs 5,668,591
    CAST(m.measurement_id AS BIGINT) AS measurement_id,
    CAST(m.measurement_uuid AS VARCHAR) AS measurement_uuid,
    CAST(m.date AS DATE) AS date,
    CAST(m.created_timestamp AS TIMESTAMP WITH TIME ZONE) AS created_timestamp,

    -- -------------------------------------------------------------------------
    -- Time Analysis Fields (Local Timezone)
    -- ALREADY COMPUTED in upstream scripts - just pass through
    -- Original computed these in final SELECT with timezone JOINs
    -- -------------------------------------------------------------------------
    CAST(local_created_timestamp AS TIMESTAMP) AS local_created_timestamp,
    CAST(local_hour_of_measurement AS BIGINT) AS local_hour_of_measurement,
    CAST(local_day_of_week AS VARCHAR) AS local_day_of_week,

    -- Weekday vs weekend flag (Monday=1 through Friday=5 are weekdays)
    CAST(is_weekday AS BOOLEAN) AS is_weekday,

    -- School hours classification: 8am-4pm vs outside school hours
    CAST(measurement_time_window AS VARCHAR) AS measurement_time_window,

    -- -------------------------------------------------------------------------
    -- Geographic Fields
    -- -------------------------------------------------------------------------
    CAST(m.iso3_code AS VARCHAR) AS iso3_code,                         -- RENAMED from iso3_format
    CAST(m.country AS VARCHAR) AS country,

    -- -------------------------------------------------------------------------
    -- School Identification
    -- -------------------------------------------------------------------------
    CAST(m.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(m.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(m.school_name AS VARCHAR) AS school_name,

    -- -------------------------------------------------------------------------
    -- Network Detection Fields
    -- -------------------------------------------------------------------------
    CAST(m.server_location AS VARCHAR) AS detected_server,
    -- ISP CLEANING: Remove ASN number from ISP name, clean up quotes
    -- Original: isp_clean (renamed to isp_name for clarity)
    CAST(REPLACE(REPLACE(m.detected_isp_raw, m.detected_isp_asn, ''), '""', '') AS VARCHAR) AS isp_name,
    -- Original: isp_asn_clean (renamed to isp_asn for clarity)
    CAST(REPLACE(REPLACE(m.detected_isp_asn, ''), '""', '') AS VARCHAR) AS isp_asn,
    CAST(m.ip_address AS VARCHAR) AS ip_address,
    CAST(m.app_version AS VARCHAR) AS app_version,
    CAST(m.notes AS VARCHAR) AS notes,

    -- -------------------------------------------------------------------------
    -- WiFi Connection Details
    -- -------------------------------------------------------------------------
    CAST(m.detected_wifi_ssid AS VARCHAR) AS detected_wifi_ssid,
    CAST(m.detected_wifi_model AS VARCHAR) AS detected_wifi_model,
    CAST(m.detected_wifi_quality AS INTEGER) AS detected_wifi_quality,
    CAST(m.detected_wifi_signal AS DECIMAL(10,2)) AS detected_wifi_signal,
    CAST(m.detected_wifi_tx_rate AS INTEGER) AS detected_wifi_tx_rate,
    CAST(m.detected_wifi_channel AS INTEGER) AS detected_wifi_channel,
    CAST(m.detected_wifi_frequency AS INTEGER) AS detected_wifi_frequency,

    -- -------------------------------------------------------------------------
    -- Device Identification
    -- -------------------------------------------------------------------------
    CAST(m.device_id AS VARCHAR) AS device_id,
    CAST(m.browser_id AS VARCHAR) AS browser_id,

    -- -------------------------------------------------------------------------
    -- Installation
    -- -------------------------------------------------------------------------
    CAST(m.installed_path AS VARCHAR) AS installed_path,
    CAST(m.windows_username AS VARCHAR) AS windows_username,

    -- -------------------------------------------------------------------------
    -- Data Source
    -- Values: 'GigaMeter' or 'Mlab'
    -- -------------------------------------------------------------------------
    CAST(m.rt_source AS VARCHAR) AS rt_source,

    -- -------------------------------------------------------------------------
    -- RAW METRICS
    -- Individual measurement values, not aggregated
    -- download_speed: kbps-based (unchanged)
    -- upload_speed:   TCPInfo bytes-based (NDT-7 C2S; NULL for ndt-5/MLab without TCPInfo)
    -- upload_speed_legacy: original kbps-based upload, kept for comparison
    -- -------------------------------------------------------------------------
   -- m.num_measurements,                 -- REMOVED: No aggregation
    CAST(m.download_speed AS REAL) AS download_speed,                    -- Raw value in Mbps
    CAST(m.upload_speed AS REAL) AS upload_speed,                      -- Raw value in Mbps (TCP-based)
    CAST(m.upload_speed_legacy AS REAL) AS upload_speed_legacy,          -- Raw value in Mbps (kbps-based, legacy)
    -- NDT-5 alternative download speed (Roberto's TCP-based formula). NULL for
    -- ~80.7% of NDT-5 tests without parseable timestamps/bytes, and for all
    -- NDT-7/MLab. Not used in download_speed or any validity flag - exposed
    -- for monitoring / potential future switch-over only.
    CAST(m.ndt5_download_speed_alt AS REAL) AS ndt5_download_speed_alt,
    CAST(m.latency AS BIGINT) AS latency,                           -- In milliseconds
    CAST(m.data_downloaded_gb AS REAL) AS data_downloaded_gb,                -- Raw value in GB
    CAST(m.data_uploaded_gb AS REAL) AS data_uploaded_gb,                  -- Raw value in GB
    CAST(m.data_usage_gb AS REAL) AS data_usage_gb,                     -- Raw value in GB

    -- -------------------------------------------------------------------------
    -- CALCULATED METRICS
    -- -------------------------------------------------------------------------

    cast(m.s2c_bytes_retrans as BIGINT) AS s2c_bytes_retrans ,
    cast(m.s2c_bytes_sent as BIGINT) AS s2c_bytes_sent ,
    cast(m.s2c_bytes_retrans as BIGINT)  / NULLIF(cast(m.s2c_bytes_sent as DECIMAL(18,6)) , 0) as packet_loss_rate,       -- NEW: download packet loss percentage

    -- -------------------------------------------------------------------------
    -- BACKWARDS COMPATIBILITY ALIASES
    -- These alias raw values as avg_*/total_* for existing dashboard compatibility
    -- TODO: Remove after single country dashboard built and tested
    -- -------------------------------------------------------------------------
    CAST(m.download_speed AS REAL) as avg_download_speed,           -- Alias: In Mbps
    CAST(m.upload_speed AS REAL) as avg_upload_speed,               -- Alias: In Mbps
    CAST(m.latency AS BIGINT) as avg_latency,                         -- Alias: In milliseconds
    CAST(m.data_downloaded_gb AS REAL) as avg_data_downloaded_gb,   -- Alias: In GB
    CAST(m.data_uploaded_gb AS REAL) as avg_data_uploaded_gb,       -- Alias: In GB
    CAST(m.data_usage_gb AS REAL) as avg_data_usage_gb,             -- Alias: In GB
    CAST(m.data_downloaded_gb AS REAL) as total_data_downloaded_gb, -- Alias: In GB
    CAST(m.data_uploaded_gb AS REAL) as total_data_uploaded_gb,     -- Alias: In GB
    CAST(m.data_usage_gb AS REAL) as total_data_usage_gb,           -- Alias: In GB

    -- -------------------------------------------------------------------------
    -- School Master Enrichment Fields
    -- Added via JOINs in measure CTE
    -- -------------------------------------------------------------------------
    CAST(m.admin1 AS VARCHAR) AS admin1,
    CAST(m.admin2 AS VARCHAR) AS admin2,
    CAST(m.connectivity AS VARCHAR) AS connectivity,
    CAST(m.connectivity_type_govt AS VARCHAR) AS connectivity_type_govt,
    CAST(m.cellular_coverage_type AS VARCHAR) AS cellular_coverage_type,
    CAST(m.education_level AS VARCHAR) AS education_level,
    CAST(m.electricity_availability AS VARCHAR) AS electricity_availability,
    CAST(m.fiber_node_distance AS DOUBLE) AS fiber_node_distance,
    CAST(m.school_area_type AS VARCHAR) AS school_area_type,
    CAST(m.school_funding_type AS VARCHAR) AS school_funding_type,
    --m.deleted,                          -- REMOVED: Filtered upstream
    CAST(m.latitude AS DOUBLE) AS latitude,
    CAST(m.longitude AS DOUBLE) AS longitude,


    -- -------------------------------------------------------------------------
    -- Geolocation (from GigaMeter app; NULL for MLab)
    -- -------------------------------------------------------------------------
    CAST(m.detected_latitude AS DOUBLE)             AS detected_latitude,
    CAST(m.detected_longitude AS DOUBLE)            AS detected_longitude,
    CAST(m.detected_location_is_flagged AS BOOLEAN) AS detected_location_is_flagged,
    CAST(m.detected_location_distance AS DOUBLE)    AS detected_location_distance,
    CAST(m.detected_location_accuracy AS DOUBLE)    AS detected_location_accuracy,


    -- -------------------------------------------------------------------------
    -- Country-Specific Fields
    -- Special enrichments for specific use cases
    -- -------------------------------------------------------------------------
    CAST(c.canton_name AS VARCHAR) AS canton_bih_only,    -- Bosnia and Herzegovina only
    CAST(cc.status AS VARCHAR) AS connectivity_credits_school,  -- Connectivity credits pilot program
    CAST(p.zaf_province AS VARCHAR) AS province_zaf_only,

    -- -------------------------------------------------------------------------
    -- Measurement Protocol
    -- 'ndt-7', 'ndt-5', or 'undetermined' (derived from JSON field presence)
    -- -------------------------------------------------------------------------
    CAST(mf.measurement_protocol AS VARCHAR) AS measurement_protocol,

    -- -------------------------------------------------------------------------
    -- Validation Flags - Categorical
    -- Detailed per-direction classification: pass / early_exit / too_short /
    -- too_long / last_snapshot_unavailable / undetermined / investigate
    -- -------------------------------------------------------------------------
    CAST(mf.download_flag AS VARCHAR) AS download_flag,
    CAST(mf.upload_flag AS VARCHAR) AS upload_flag,

    -- -------------------------------------------------------------------------
    -- Validation Flags - Overall
    -- -------------------------------------------------------------------------
    CAST(mf.pass_fail_overall AS VARCHAR) AS pass_fail_overall,
    CAST(mf.reasons_failed_overall AS VARCHAR) AS reasons_failed_overall,

    -- -------------------------------------------------------------------------
    -- Validation Flags - Download
    -- -------------------------------------------------------------------------
    CAST(mf.pass_fail_download AS VARCHAR) AS pass_fail_download,
    CAST(mf.reasons_failed_download AS VARCHAR) AS reasons_failed_download,

    -- -------------------------------------------------------------------------
    -- Validation Flags - Upload
    -- -------------------------------------------------------------------------
    CAST(mf.pass_fail_upload AS VARCHAR) AS pass_fail_upload,
    CAST(mf.reasons_failed_upload AS VARCHAR) AS reasons_failed_upload
FROM
  measure m
LEFT JOIN
  delta_lake.test_tables.all_gigameter_valid_test_checker_incremental  mf
on
  m.measurement_id = mf.measurement_id
-- JOIN: Bosnia canton mapping (country-specific enrichment)
-- Only populates for schools in Bosnia and Herzegovina
LEFT JOIN lstringer.bih_school_canton_mapping_vw c
    ON m.school_id_giga = c.school_id_giga

-- JOIN: Connectivity credits pilot program tracking
-- Identifies schools participating in connectivity credit pilot
LEFT JOIN lstringer.connectivity_credit_pilot_schools cc
    ON m.school_id_giga = cc.school_id_giga

LEFT JOIN lstringer.zaf_province_mapping p
  on m.school_id_giga = p.school_id_giga
