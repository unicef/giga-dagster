


-- ==============================================================================
-- Script Name:     all_gigameter_appversion_funnel.sql
-- Table Created:   default.all_gigameter_appversion_funnel
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Device-level funnel analysis tracking GigaMeter app adoption across three
--   stages: (1) device registered, (2) first data sent, (3) app version upgrades.
--   Records initial, first-measured, and current app versions per device-school
--   combination, enabling upgrade-rate and adoption tracking over time.
--
-- Dependencies:
--   - gigameter_production_db.public.device (device registration records)
--   - gigameter_production_db.public.measurements (measurement history)
--   - gigameter_production_db.public.school (school registration)
--   - default.all_school_master (school metadata and geography)
--
-- Output Columns:  ~20 columns
-- Primary Key:     device_id + school_id_giga
-- Granularity:     One row per device-school combination
--
-- Run Notes:
--   Recurring — refresh daily. Consumed by all_gigameter_registered_devices.sql
--   for version tracking. ROW_NUMBER() de-duplication handles devices associated
--   with multiple schools.
--
-- ==============================================================================

  CREATE TABLE IF NOT EXISTS default.all_gigameter_appversion_funnel as (

-- CREATE or replace view default.all_gigameter_appversion_funnel_vw as
-- ============================================================
-- GigaMeter App Version Funnel Analysis - Version 1 (Fixed)
-- ============================================================
-- This query analyzes the funnel of GigaMeter app usage:
-- 1. Device registration → 2. First data sent → 3. App version upgrades
--
-- FIXED: Added school constraints to prevent duplicate rows
-- and ensure proper device-school relationship handling
-- ============================================================

WITH

-- ============================================================
-- CTE 1: devices_distinct
-- Get the list of devices sending data + their school metadata
-- ============================================================
devices_distinct AS (

    SELECT
      DISTINCT
        dcs.giga_id_school AS school_id_giga,
        dcs.device_hardware_id AS device_id,
        dcs.user_id,
        sm.school_id_govt,
        sm.iso3_code,
        sm.country,
        sm.unicef_region,
        CAST(SUBSTRING(dcs.created, 1, 10) AS date) AS initial_registration_date,
        dcs.app_version AS initial_app_version,
        ROW_NUMBER() OVER (PARTITION BY dcs.giga_id_school ORDER BY dcs.created ASC) AS row_num,
        'GigaMeter' AS rt_source
    FROM gigameter_production_db.public.dailycheckapp_school dcs
    LEFT JOIN default.all_school_master sm
           ON dcs.giga_id_school = sm.school_id_giga

)

-- ============================================================
-- CTE 2: initial
-- Get all GigaMeter measurement records and parse app versions
-- ============================================================
, initial AS (
    SELECT
        school_id_govt,
        school_id_giga,
        device_id,
        browser_id,
        app_version AS last_app_version,
        TRY_CAST(SPLIT_PART(app_version, '.', 1) AS integer) AS last_app_version_major,
        TRY_CAST(SPLIT_PART(app_version, '.', 2) AS integer) AS last_app_version_minor,
        TRY_CAST(SPLIT_PART(app_version, '.', 3) AS integer) AS last_app_version_patch,
        created_timestamp AS measurement_date,
        rt_source
    FROM default.all_gigameter_measurement_data
    WHERE rt_source = 'GigaMeter'
)

-- ============================================================
-- CTE 3: base
-- Add row numbers to identify first/last measurements and upgrades
-- ============================================================
, base AS (
    SELECT
        school_id_govt,
        school_id_giga,
        device_id,
        browser_id,
        last_app_version,
        last_app_version_major,
        last_app_version_minor,
        last_app_version_patch,
        measurement_date,
        rt_source,

        -- Latest measurement per device-school combination
        ROW_NUMBER() OVER (PARTITION BY browser_id, school_id_giga ORDER BY measurement_date DESC) AS row_num_desc,

        -- First measurement per device-school combination
        ROW_NUMBER() OVER (PARTITION BY browser_id, school_id_giga ORDER BY measurement_date ASC) AS row_num_asc,

        -- First measurement per (device, school, major version, patch) = upgrade detection
        ROW_NUMBER() OVER (
            PARTITION BY browser_id, school_id_giga, last_app_version_major, last_app_version_patch
            ORDER BY measurement_date ASC
        ) AS app_upgrade_row_num
    FROM initial
)

-- ============================================================
-- CTE 4: gmeter_last_measurement_date
-- Get the most recent measurement for each device-school combination
-- ============================================================
, gmeter_last_measurement_date AS (
    SELECT
        base.school_id_govt,
        base.school_id_giga,
        base.device_id,
        base.browser_id,
        base.last_app_version,
        measurement_date AS last_measurement_date,
        row_num_desc,
        rt_source
    FROM base
    WHERE row_num_desc = 1
)

-- ============================================================
-- CTE 5: gmeter_first_measurement_date
-- Get the earliest measurement for each device-school combination
-- ============================================================
, gmeter_first_measurement_date AS (
    SELECT
        base.school_id_govt,
        base.school_id_giga,
        base.device_id,
        base.browser_id,
        base.last_app_version AS first_app_version,
        measurement_date AS first_measurement_date,
        row_num_asc,
        rt_source
    FROM base
    WHERE row_num_asc = 1
)

-- ============================================================
-- CTE 6: gmeter_version_upgrade_date
-- Get version upgrade events (first time each version was used)
-- ============================================================
, gmeter_version_upgrade_date AS (
    SELECT
        base.school_id_govt,
        base.school_id_giga,
        base.device_id,
        base.browser_id,
        base.last_app_version AS app_version,
        last_app_version_major,
        last_app_version_patch,
        measurement_date AS version_update_date,
        app_upgrade_row_num,
        rt_source,
        ROW_NUMBER() OVER (
            PARTITION BY browser_id, school_id_giga
            ORDER BY last_app_version_major DESC, last_app_version_patch DESC
        ) AS last_app_version_num
    FROM base
    WHERE app_upgrade_row_num = 1
)


-- ============================================================
-- Final SELECT: Join all data sources and create funnel analysis
-- ============================================================
-- FIXED: Now includes school constraints in all joins to ensure:
-- 1. One row per device-school combination (no duplicates)
-- 2. Proper version upgrade data matching device-school pairs
-- ============================================================
SELECT DISTINCT
    -- Funnel flags
    CAST(CASE WHEN d.initial_registration_date IS NULL THEN 'no' ELSE 'yes' END AS VARCHAR) AS registered_gigameter,
    CAST(CASE WHEN i.first_measurement_date IS NULL THEN 'no' ELSE 'yes' END AS VARCHAR) AS sent_gigameter_data,

    -- School and device identifiers
    CAST(d.country AS VARCHAR) AS country,
    CAST(d.unicef_region AS VARCHAR) AS unicef_region,
    CAST(d.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(d.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(d.device_id AS VARCHAR) AS device_id,
    CAST(d.user_id AS VARCHAR) as browser_id,

    -- RT source identification
    CAST(CASE WHEN lower(d.initial_app_version) LIKE '%mlab%' THEN 'mlab' ELSE 'GigaMeter' END AS VARCHAR) AS initial_rt_source,

    -- Clean version values (for downstream processing)
    CAST(d.initial_app_version AS VARCHAR) AS initial_app_version_clean,
    CAST(i.first_app_version AS VARCHAR) AS first_app_version_measured_clean,
    CAST(vu.app_version AS VARCHAR) AS most_recent_app_version_clean,

    -- Important dates
    CAST(i.first_measurement_date AS TIMESTAMP) AS first_measurement_date,
    CAST(d.initial_registration_date AS DATE) AS initial_registration_date,
    CAST(r.last_measurement_date AS TIMESTAMP) AS last_measurement_date,
    CAST(vu.version_update_date AS TIMESTAMP) AS version_update_date,

    -- Version upgrade information
    CAST(vu.app_version AS VARCHAR) AS version_updated_to,

    -- Formatted version strings (for display)
    CAST(COALESCE(NULLIF(d.initial_app_version, '') || ' (initial)', 'null (initial)') AS VARCHAR) AS initial_app_version,
    CAST(COALESCE(NULLIF(i.first_app_version, '') || ' (first)', 'null (first)') AS VARCHAR) AS first_app_version_measured,
    CAST(COALESCE(NULLIF(vu.app_version, '') || ' (current)', 'null (current)') AS VARCHAR) AS most_recent_app_version

FROM devices_distinct d
INNER JOIN gmeter_first_measurement_date i
       ON d.user_id = i.browser_id
      AND d.school_id_giga = i.school_id_giga
INNER JOIN gmeter_last_measurement_date r
       ON d.user_id = r.browser_id
      AND d.school_id_giga = r.school_id_giga
INNER JOIN gmeter_version_upgrade_date vu
       ON d.user_id = vu.browser_id
      AND d.school_id_giga = vu.school_id_giga
      AND vu.last_app_version_num = 1


 )
