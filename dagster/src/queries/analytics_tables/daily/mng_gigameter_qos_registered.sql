



-- ==============================================================================
-- Script Name:     mng_gigameter_qos_registered.sql
-- Table Created:   default.mng_gigameter_qos_registered
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   School-level registration summary for Mongolia combining two data sources:
--   1. LibreRouter (libre) - Hardware-based monitoring device registration
--   2. GigaMeter app registrations
--   Provides a unified view of which schools are registered with which
--   monitoring system and their measurement history.
--
-- Dependencies:
--   - qos_raw.mng (raw LibreRouter data for Mongolia)
--   - default.all_gigameter_registered_tb_physical (GigaMeter registered schools)
--   - school_master.mng (Mongolia school master data)
--
-- Output Columns:  ~20 columns
-- Primary Key:     school_id_giga
-- Granularity:     One row per registered school
--
-- CAVEATS:
--   - rtm_source indicates registration source:
--     * 'both' = registered with both LibreRouter and GigaMeter
--     * 'gigameter' = GigaMeter only
--     * 'libre' = LibreRouter only
--     * 'none' = no registration (shouldn't occur)
--   - Manual date override for school '33683f1d-f143-3704-81a1-53a8229d9e61'
--     to set first_measurement_date to '2022-12-31 02:00:40'
--   - days_since_last_measurement uses GREATEST() to find most recent
--     measurement from either source
--   - Only includes schools with initial_registration_date (GigaMeter) or
--     speed_download IS NOT NULL (LibreRouter)
--   - Uses qos_raw.mng (not aggregated) for detailed device-level tracking
--
-- Last Updated:    [Not specified in CSV]
-- ==============================================================================



CREATE TABLE IF NOT EXISTS default.mng_gigameter_qos_registered AS (

WITH

-- ==============================================================================
-- CTE: libre
-- Purpose: LibreRouter device registrations for Mongolia
-- Source:  qos_raw.mng (raw data for device-level detail)
-- Filter:  speed_download IS NOT NULL (only valid measurements)
-- Metrics:
--   - num_measurements: Total measurements per school/device
--   - num_days_measured: Distinct days with measurements
--   - first/last_measurement_date: Registration activity window
-- ==============================================================================
libre AS (
    SELECT DISTINCT
        country_id,
        school_id_giga,
        school_id_govt,
        'libre' AS provider,
        device_id,
        COUNT(*) AS num_measurements,
        COUNT(DISTINCT date) AS num_days_measured,
        MIN(timestamp) AS first_measurement_date,
        MAX(timestamp) AS last_measurement_date,
        1 AS libre_source                       -- Flag for join logic
    FROM qos_raw.mng
    WHERE speed_download IS NOT NULL            -- Only valid measurements
    GROUP BY
        country_id,
        school_id_govt,
        school_id_giga,
        device_id
),


-- ==============================================================================
-- CTE: gigameter_registered
-- Purpose: GigaMeter app registrations for Mongolia
-- Source:  default.all_gigameter_registered_tb_physical
-- Filter:  country = 'Mongolia' AND initial_registration_date IS NOT NULL
-- ==============================================================================
gigameter_registered AS (
    SELECT
        school_id_giga,
        initial_registration_date,
        first_measurement_date,
        last_measurement_date,
        latest_measurement_app_version,
        highest_app_version_in_use,
        num_measurements,
        num_days_measured,
        num_registration_attempts,
        primary_server,
        primary_isp,
        primary_isp_asn,
        secondary_server,
        secondary_isp,
        secondary_isp_asn,
        primary_source,
        1 AS gigameter_source                   -- Flag for join logic
    FROM default.all_gigameter_registered_tb_physical
    WHERE country = 'Mongolia'
        AND initial_registration_date IS NOT NULL
),


-- ==============================================================================
-- CTE: e (enriched)
-- Purpose: Combines LibreRouter and GigaMeter registrations via FULL OUTER JOIN
-- Transforms:
--   - rtm_source: Indicates which system(s) the school is registered with
--   - Separate columns for metrics from each source
--   - Manual date override for specific school (data correction)
--   - days_since_last_measurement: Uses GREATEST() to find most recent
--     measurement date from either source
-- ==============================================================================
e AS (
    SELECT
        COALESCE(gigameter_registered.school_id_giga, libre.school_id_giga) AS school_id_giga,

        -- Registration source indicator
        CASE
            WHEN libre.libre_source = 1 AND gigameter_registered.gigameter_source = 1 THEN 'both'
            WHEN gigameter_registered.gigameter_source = 1 AND libre.libre_source IS NULL THEN 'gigameter'
            WHEN libre.libre_source = 1 AND gigameter_registered.gigameter_source IS NULL THEN 'libre'
            ELSE 'none'
        END AS rtm_source,

        -- GigaMeter registration fields
        gigameter_registered.initial_registration_date AS gigameter_initial_registration_date,

        -- Manual date override for specific school (data correction)
        -- CAVEAT: School '33683f1d-f143-3704-81a1-53a8229d9e61' has corrected date
        CASE
            WHEN gigameter_registered.school_id_giga IN ('33683f1d-f143-3704-81a1-53a8229d9e61')
            THEN TIMESTAMP '2022-12-31 02:00:40'
            ELSE gigameter_registered.first_measurement_date
        END AS gigameter_first_measurement_date,

        gigameter_registered.last_measurement_date AS gigameter_last_measurement_date,
        gigameter_registered.latest_measurement_app_version AS gigameter_last_app_version,
        gigameter_registered.highest_app_version_in_use AS gigameter_highest_app_version,
        gigameter_registered.num_measurements AS gigameter_num_measurements,
        gigameter_registered.num_days_measured AS gigameter_num_days_measured,
        gigameter_registered.num_registration_attempts AS gigameter_num_registration_attempts,
        gigameter_registered.primary_server AS gigameter_primary_server,
        gigameter_registered.primary_isp AS gigameter_primary_isp,
        gigameter_registered.secondary_isp AS gigameter_secondary_isp,

        -- LibreRouter registration fields
        libre.first_measurement_date AS libre_first_measurement_date,
        libre.last_measurement_date AS libre_last_measurement_date,
        libre.num_measurements AS libre_num_measurements,
        libre.num_days_measured AS libre_num_days_measured,

        -- Calculate days since most recent measurement from either source
        -- Uses GREATEST() to find the most recent timestamp
        DATE_DIFF('day',
            GREATEST(
                COALESCE(gigameter_registered.last_measurement_date, TIMESTAMP '1970-01-01'),
                COALESCE(libre.last_measurement_date, TIMESTAMP '1970-01-01')
            ),
            current_date
        ) AS days_since_last_measurement

    FROM gigameter_registered
    FULL OUTER JOIN libre
        ON gigameter_registered.school_id_giga = libre.school_id_giga
)


-- ==============================================================================
-- FINAL SELECT
-- Purpose: Enriches registration data with school master information
-- ==============================================================================
SELECT
    CAST(e.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(e.rtm_source AS VARCHAR) AS rtm_source,
    CAST(e.gigameter_initial_registration_date AS DATE) AS gigameter_initial_registration_date,
    CAST(e.gigameter_first_measurement_date AS TIMESTAMP) AS gigameter_first_measurement_date,
    CAST(e.gigameter_last_measurement_date AS TIMESTAMP) AS gigameter_last_measurement_date,
    CAST(e.gigameter_last_app_version AS VARCHAR) AS gigameter_last_app_version,
    CAST(e.gigameter_highest_app_version AS VARCHAR) AS gigameter_highest_app_version,
    CAST(e.gigameter_num_measurements AS BIGINT) AS gigameter_num_measurements,
    CAST(e.gigameter_num_days_measured AS BIGINT) AS gigameter_num_days_measured,
    CAST(e.gigameter_num_registration_attempts AS BIGINT) AS gigameter_num_registration_attempts,
    CAST(e.gigameter_primary_server AS VARCHAR) AS gigameter_primary_server,
    CAST(e.gigameter_primary_isp AS VARCHAR) AS gigameter_primary_isp,
    CAST(e.gigameter_secondary_isp AS VARCHAR) AS gigameter_secondary_isp,
    CAST(e.libre_first_measurement_date AS TIMESTAMP) AS libre_first_measurement_date,
    CAST(e.libre_last_measurement_date AS TIMESTAMP) AS libre_last_measurement_date,
    CAST(e.libre_num_measurements AS BIGINT) AS libre_num_measurements,
    CAST(e.libre_num_days_measured AS BIGINT) AS libre_num_days_measured,
    CAST(e.days_since_last_measurement AS BIGINT) AS days_since_last_measurement,
    CAST(master.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(master.school_name AS VARCHAR) AS school_name,
    CAST(master.admin1 AS VARCHAR) AS admin1,                              -- Province (Aimag)
    CAST(master.admin2 AS VARCHAR) AS admin2,                              -- District (Soum)
    CAST(master.connectivity AS VARCHAR) AS connectivity,
    CAST(master.connectivity_type_govt AS VARCHAR) AS connectivity_type_govt,
    CAST(master.cellular_coverage_type AS VARCHAR) AS cellular_coverage_type,
    CAST(master.education_level AS VARCHAR) AS education_level,
    CAST(master.electricity_availability AS VARCHAR) AS electricity_availability,
    CAST(master.fiber_node_distance AS DOUBLE) AS fiber_node_distance
FROM e
LEFT JOIN school_master.mng master
    ON master.school_id_giga = e.school_id_giga

);
