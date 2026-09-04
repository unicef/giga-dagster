-- ==============================================================================
-- Script Name:     all_gigameter_measurement_data_tb_physical.sql
-- Table Created:   default.all_gigameter_measurement_data_tb_physical
-- Schema:          default
-- Pipeline Step:   3 of 3 (Final) - Backwards Compatible Version
--
-- Purpose:
--   Backwards-compatible version of the final measurement data script that
--   produces output matching the original all_gigameter_measurement_data_tb_physical
--   schema exactly. Use this script when migrating from the old monolithic
--   script to the new 3-script pipeline while maintaining schema compatibility.
--
-- Key Features:
--   1. Uses same CTE structure as refactored pipeline (UNION of upstream tables)
--   2. Maintains all original column names (iso3_format, isp_clean, isp_asn_clean)
--   3. Includes restored columns: hour_of_measurement, num_measurements, deleted
--   4. Output schema matches original script exactly (~50 columns)
--
-- Dependencies:
--   - default.all_gmeter_only_measurements (Script 1 output)
--   - default.all_mlab_only_measurements (Script 2 output)
--   - default.all_school_master (school metadata enrichment)
--   - lstringer.bih_school_canton_mapping_vw (Bosnia canton - optional)
--   - lstringer.connectivity_credit_pilot_schools (pilot program - optional)
--
-- Output Columns:  ~50 columns (matches original schema)
-- Primary Key:     measurement_id
--
-- Author:          Refactored by Luke Stringer
-- Last Updated:    2025-01-27
-- ==============================================================================


DROP TABLE IF EXISTS default.all_gigameter_measurement_data_tb_physical;

CREATE TABLE IF NOT EXISTS default.all_gigameter_measurement_data_tb_physical
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_measurement_data_tb_physical'
)
AS (


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
-- =============================================================================
WITH  measure AS (

    -- =========================================================================
    -- PART 1: GigaMeter Measurements
    -- Source: default.all_gmeter_only_measurements
    -- Join: school_id_giga (reliable, from GigaMeter app registration)
    -- =========================================================================
    SELECT
      m1.*,                              -- All columns from upstream table
      -- School master enrichment fields
      s1.admin1,
      s1.admin2,
      s1.connectivity,
      s1.connectivity_type,
      s1.cellular_coverage_type,
      s1.education_level,
      s1.electricity_availability,
      s1.fiber_node_distance,
      s1.school_area_type,
      s1.school_funding_type,
      s1.latitude,
      s1.longitude
    FROM
      default.all_gmeter_only_measurements m1
    LEFT JOIN
      default.all_school_master s1
    on
      -- GigaMeter uses reliable giga_id for matching
      m1.school_id_giga = s1.school_id_giga


UNION ALL


    -- =========================================================================
    -- PART 2: MLAB Measurements
    -- Source: default.all_mlab_only_measurements
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
      s2.connectivity_type,
      s2.cellular_coverage_type,
      s2.education_level,
      s2.electricity_availability,
      s2.fiber_node_distance,
      s2.school_area_type,
      s2.school_funding_type,
      s2.latitude,
      s2.longitude
    FROM
      default.all_mlab_only_measurements m2
    LEFT JOIN
      default.all_school_master s2
    on
      -- MLAB requires compound key for reliable matching
      m2.school_id_govt = s2.school_id_govt
    and
      m2.iso3_code = s2.iso3_code


)


-- =============================================================================
-- FINAL SELECT
-- Purpose: Final column selection with:
--   - Original column names preserved (iso3_format, isp_clean, isp_asn_clean)
--   - Restored columns (hour_of_measurement, num_measurements, deleted)
--   - Country-specific enrichments (Bosnia, connectivity credits)
--
-- BACKWARDS COMPATIBILITY:
--   - All columns match original all_gigameter_measurement_data_tb_physical schema
--   - Column names unchanged from original
--   - avg_* and total_* columns present (aliased from raw values)
-- =============================================================================


SELECT
 -- DISTINCT                               -- Deduplication (was via GROUP BY in original):
    m.measurement_id,
    m.date,
    m.created_timestamp,

    -- -------------------------------------------------------------------------
    -- Time Analysis Fields (UTC)
    -- RESTORED: hour_of_measurement from original schema
    -- -------------------------------------------------------------------------
    EXTRACT(HOUR FROM m.created_timestamp) AS hour_of_measurement,

    -- -------------------------------------------------------------------------
    -- Time Analysis Fields (Local Timezone)
    -- ALREADY COMPUTED in upstream scripts - just pass through
    -- Original computed these in final SELECT with timezone JOINs
    -- -------------------------------------------------------------------------
    local_created_timestamp,
    local_hour_of_measurement,
    local_day_of_week,

    -- Weekday vs weekend flag (Monday=1 through Friday=5 are weekdays)
    is_weekday,

    -- School hours classification: 8am-4pm vs outside school hours
    measurement_time_window,

    -- -------------------------------------------------------------------------
    -- Geographic Fields
    -- ORIGINAL NAME: iso3_format (not iso3_code)
    -- -------------------------------------------------------------------------
    m.iso3_code AS iso3_format,
    m.country,

    -- -------------------------------------------------------------------------
    -- School Identification
    -- -------------------------------------------------------------------------
    m.school_id_giga,
    m.school_id_govt,
    m.school_name,

    -- -------------------------------------------------------------------------
    -- Network Detection Fields
    -- ORIGINAL NAMES: isp_clean, isp_asn_clean (not isp_name, isp_asn)
    -- -------------------------------------------------------------------------
    m.server_location as detected_server,
    -- ISP CLEANING: Remove ASN number from ISP name, clean up quotes
    REPLACE(REPLACE(m.detected_isp_raw, m.detected_isp_asn, ''), '""', '') AS isp_clean,
    REPLACE(REPLACE(m.detected_isp_asn, ''), '""', '') AS isp_asn_clean,
    m.app_version,
    m.notes,

    -- -------------------------------------------------------------------------
    -- WiFi Connection Details
    -- -------------------------------------------------------------------------
    m.detected_wifi_ssid,
    m.detected_wifi_model,
    m.detected_wifi_quality,
    m.detected_wifi_signal,
    m.detected_wifi_tx_rate,
    m.detected_wifi_channel,
    m.detected_wifi_frequency,

    -- -------------------------------------------------------------------------
    -- Device Identification
    -- -------------------------------------------------------------------------
    m.device_id,
    m.browser_id,

    -- -------------------------------------------------------------------------
    -- Data Source
    -- Values: 'GigaMeter' or 'Mlab'
    -- -------------------------------------------------------------------------
    m.rt_source,

    -- -------------------------------------------------------------------------
    -- Measurement Count
    -- RESTORED: num_measurements from original schema
    -- Hard-coded to 1 since each row represents a single measurement
    -- (Original script aggregated, so this was COUNT(*))
    -- -------------------------------------------------------------------------
    1 AS num_measurements,

    -- -------------------------------------------------------------------------
    -- Measurement Metrics
    -- Aliased as avg_* and total_* for original schema compatibility
    -- Since each row is one measurement, avg = total = raw value
    -- -------------------------------------------------------------------------
    m.download_speed AS avg_download_speed,           -- In Mbps
    m.upload_speed AS avg_upload_speed,               -- In Mbps
    m.latency AS avg_latency,                         -- In milliseconds
    m.data_downloaded_gb AS avg_data_downloaded_gb,
    m.data_uploaded_gb AS avg_data_uploaded_gb,
    m.data_usage_gb AS avg_data_usage_gb,
    m.data_downloaded_gb AS total_data_downloaded_gb,
    m.data_uploaded_gb AS total_data_uploaded_gb,
    m.data_usage_gb AS total_data_usage_gb,


    -- -------------------------------------------------------------------------
    -- CALCULATED METRICS
    -- -------------------------------------------------------------------------
    cast(m.s2c_bytes_retrans as BIGINT) AS s2c_bytes_retrans ,
    cast(m.s2c_bytes_sent as BIGINT) AS s2c_bytes_sent ,
    cast(m.s2c_bytes_retrans as BIGINT)  / NULLIF(cast(m.s2c_bytes_sent as DECIMAL(18,6)) , 0) as packet_loss_rate,       -- NEW: download packet loss percentage

    -- -------------------------------------------------------------------------
    -- School Master Enrichment Fields
    -- Added via JOINs in measure CTE
    -- -------------------------------------------------------------------------
    m.admin1,
    m.admin2,
    m.connectivity,
    m.connectivity_type,
    m.cellular_coverage_type,
    m.education_level,
    m.electricity_availability,
    m.fiber_node_distance,
    m.school_area_type,
    m.school_funding_type,

    -- -------------------------------------------------------------------------
    -- Deleted Column
    -- RESTORED: deleted from original schema
    -- Set to NULL since deleted records are filtered in upstream scripts
    -- Column preserved for schema compatibility with downstream consumers
    -- -------------------------------------------------------------------------
    CAST(NULL AS DATE) AS deleted,

    m.latitude,
    m.longitude,

    -- -------------------------------------------------------------------------
    -- Country-Specific Fields
    -- Special enrichments for specific use cases
    -- -------------------------------------------------------------------------
    c.canton_name AS canton_bih_only,    -- Bosnia and Herzegovina only
    cc.status AS connectivity_credits_school,  -- Connectivity credits pilot program
    p.zaf_province as province_zaf_only,
    l.zone as education_zone_lka_only

FROM measure m

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

LEFT JOIN lstringer.lka_school_education_zones l
  on m.school_id_govt = l.school_id_govt
  and m.iso3_code = l.iso3_code

-- where m.date > cast('2024-01-01' as date)       -- added
)


-- =============================================================================
-- EXECUTION ORDER
--
-- This script must be run AFTER Scripts 1 and 2 complete:
--   1. Run all_gmeter_only_measurements.sql -> Creates source table
--   2. Run all_mlab_only_measurements.sql -> Creates source table
--   3. Run this script -> Creates final combined table
--
-- MEMORY BENEFIT:
--   Original script ran all logic in single query (memory pressure)
--   Now distributed across 3 queries with intermediate materialization
-- =============================================================================


-- =============================================================================
-- SCHEMA COMPATIBILITY NOTES
--
-- This script produces output matching the original
-- all_gigameter_measurement_data_tb_physical.sql schema exactly.
--
-- Columns restored from original (not in refactored version):
--   | Column             | Value/Source                              |
--   |--------------------|------º-------------------------------------|
--   | hour_of_measurement| EXTRACT(HOUR FROM created_timestamp) UTC  |
--   | num_measurements   | 1 (each row = 1 measurement)              |
--   | deleted            | NULL (filtered upstream)                  |
--
-- Column names matching original (different in refactored version):
--   | This Script    | Refactored Script |
--   |----------------|-------------------|
--   | iso3_format    | iso3_code         |
--   | isp_clean      | isp_name          |
--   | isp_asn_clean  | isp_asn           |
--
-- Use this script for backwards compatibility with existing dashboards
-- and downstream consumers that depend on the original schema.
-- =============================================================================
