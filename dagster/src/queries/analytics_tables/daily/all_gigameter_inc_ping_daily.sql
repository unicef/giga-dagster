-- ==============================================================================
-- Script Name:     all_gigameter_inc_ping_daily.sql
-- Table Created:   default.all_gigameter_inc_ping_daily
-- Schema:          default
-- Pipeline Step:   2 of 2 (joins ping daily + speed test measurements)
--
-- Purpose:
--   Final daily table combining pre-aggregated ping data with GigaMeter speed
--   test measurements. This is Step 2 of a two-step replacement for the original
--   all_gigameter_inc_ping_daily.sql, which was exceeding Trino's 12GB memory limit.
--
-- Dependencies:
--   - default.all_ping_daily              (Step 1 output — run first)
--   - default.all_gigameter_measurement_data  (replaces _tb_physical source)
--   - default.all_gigameter_appversion_funnel
--
-- Key Changes vs Original:
--   - Ping data sourced from default.all_ping_daily (pre-aggregated, no raw CTEs)
--   - Measurement source: all_gigameter_measurement_data (not _tb_physical)
--     Column rename: iso3_format → iso3_code
--     deleted column removed (filtered upstream in measurement pipeline)
--   - first_ping_recorded filter already applied in all_ping_daily; removed here
--
-- Output columns match original all_gigameter_inc_ping_daily except:
--   - is_connected_all dropped (COUNT DISTINCT uuid; cannot reconstruct from hourly)
--
-- Pipeline order:
--   all_ping_hourly  →  all_ping_daily  →  [this script]
-- ==============================================================================


CREATE TABLE IF NOT EXISTS default.all_gigameter_inc_ping_daily AS


-- ==============================================================================
-- CTE: gmeter_aggr
-- Purpose: Aggregate GigaMeter speed test measurements to daily level per device.
-- Source changed from all_gigameter_measurement_data_tb_physical to
-- all_gigameter_measurement_data (active table; deleted rows already excluded).
-- ==============================================================================
WITH gmeter_aggr AS (
    SELECT
        CAST(local_created_timestamp AS DATE) AS local_created_date,
        country,
        iso3_code,
        school_id_giga,
        school_id_govt,
        school_name,
        app_version,
        admin1,
        admin2,
        device_id,
        rt_source,

        COUNT(*) AS measurement_records,

        AVG(download_speed)     AS avg_download_speed,
        AVG(upload_speed)       AS avg_upload_speed,
        AVG(latency)            AS avg_latency,

        AVG(data_downloaded_gb) AS avg_data_downloaded_gb,
        AVG(data_uploaded_gb)   AS avg_data_uploaded_gb,
        AVG(data_usage_gb)      AS avg_data_usage_gb,

        SUM(data_downloaded_gb) AS total_data_downloaded_gb,
        SUM(data_uploaded_gb)   AS total_data_uploaded_gb,
        SUM(data_usage_gb)      AS total_data_usage_gb,

        -- Test type counters (daily totals)
        SUM(CASE WHEN notes = 'startup' THEN 1 ELSE 0 END)                          AS notes_startup_count,
        SUM(CASE WHEN notes = 'daily'   THEN 1 ELSE 0 END)                          AS notes_daily_count,
        SUM(CASE WHEN notes = 'manual'  THEN 1 ELSE 0 END)                          AS notes_manual_count,
        SUM(CASE WHEN notes NOT IN ('startup', 'daily', 'manual') THEN 1 ELSE 0 END) AS notes_other_count

    FROM default.all_gigameter_measurement_data
    WHERE rt_source = 'GigaMeter'
      AND CAST(local_created_timestamp AS DATE) > CAST('2025-05-01' AS DATE)

    GROUP BY
        CAST(local_created_timestamp AS DATE),
        country,
        iso3_code,
        school_id_giga,
        school_id_govt,
        school_name,
        app_version,
        admin1,
        admin2,
        device_id,
        rt_source
)


-- ==============================================================================
-- FINAL SELECT
-- Purpose: FULL JOIN pre-aggregated ping data with daily measurement aggregates.
-- Note: first_ping filter is already applied inside all_ping_daily.
-- ==============================================================================
SELECT
    CAST(COALESCE(p.local_created_date, m.local_created_date) AS DATE) AS local_created_date,

    -- Data availability flag
    CAST(CASE WHEN p.ping_records IS NULL THEN 'yes' ELSE 'no' END AS VARCHAR) AS ping_records_null,

    -- Ping completeness (passed through from all_ping_daily)
    CAST(p.expected_pings_per_day AS INTEGER)                           AS expected_pings_per_day,
    CAST(p.missing_pings          AS BIGINT)                            AS missing_pings,

    -- Common identifiers (COALESCE covers measurement-only rows)
    CAST(COALESCE(p.device_id,      m.device_id)      AS VARCHAR)       AS device_id,
    CAST(COALESCE(p.school_id_govt, m.school_id_govt) AS VARCHAR)       AS school_id_govt,
    CAST(COALESCE(p.school_id_giga, m.school_id_giga) AS VARCHAR)       AS school_id_giga,
    CAST(COALESCE(p.school_name,    m.school_name)    AS VARCHAR)       AS school_name,
    CAST(COALESCE(p.country,        m.country)        AS VARCHAR)       AS country,
    CAST(COALESCE(p.admin1,         m.admin1)         AS VARCHAR)       AS admin1,
    CAST(COALESCE(p.admin2,         m.admin2)         AS VARCHAR)       AS admin2,

    -- App version: prefer measurement record; fall back to funnel lookup
    CAST(COALESCE(m.app_version, a.most_recent_app_version_clean) AS VARCHAR) AS app_version,

    -- Ping metrics (from all_ping_daily)
    CAST(p.ping_records                   AS BIGINT)                    AS ping_records,
    CAST(p.pings_8am_to_8pm_local         AS BIGINT)                    AS pings_8am_to_8pm_local,
    CAST(p.connected_8am_to_8pm_local     AS BIGINT)                    AS connected_8am_to_8pm_local,
    CAST(p.not_connected_8am_to_8pm_local AS BIGINT)                    AS not_connected_8am_to_8pm_local,
    CAST(p.invalid_ping_9pm_to_7am_local  AS BIGINT)                    AS invalid_ping_9pm_to_7am_local,
    CAST(p.connected_9pm_to_7am_local     AS BIGINT)                    AS connected_9pm_to_7am_local,
    CAST(p.not_connected_9pm_to_7am_local AS BIGINT)                    AS not_connected_9pm_to_7am_local,
    CAST(p.latency_ping                   AS BIGINT)                    AS latency_ping,
    CAST(p.uptime                         AS DOUBLE)                    AS uptime,
    CAST(p.uptime_8am_to_8pm_local        AS DOUBLE)                    AS uptime_8am_to_8pm_local,

    -- GigaMeter speed test metrics (from gmeter_aggr)
    CAST(m.measurement_records      AS BIGINT)                          AS measurement_records,
    CAST(m.avg_download_speed       AS REAL)                            AS download_speed,
    CAST(m.avg_upload_speed         AS REAL)                            AS upload_speed,
    CAST(m.avg_latency              AS BIGINT)                          AS latency_meter,
    CAST(m.avg_data_downloaded_gb   AS REAL)                            AS avg_data_downloaded_gb,
    CAST(m.avg_data_uploaded_gb     AS REAL)                            AS avg_data_uploaded_gb,
    CAST(m.avg_data_usage_gb        AS REAL)                            AS avg_data_usage_gb,
    CAST(m.total_data_downloaded_gb AS REAL)                            AS total_data_downloaded_gb,
    CAST(m.total_data_uploaded_gb   AS REAL)                            AS total_data_uploaded_gb,
    CAST(m.total_data_usage_gb      AS REAL)                            AS total_data_usage_gb,
    CAST(m.notes_startup_count      AS BIGINT)                          AS notes_startup_count,
    CAST(m.notes_daily_count        AS BIGINT)                          AS notes_daily_count,
    CAST(m.notes_manual_count       AS BIGINT)                          AS notes_manual_count,
    CAST(m.notes_other_count        AS BIGINT)                          AS notes_other_count,

    -- App version components
    TRY_CAST(split_part(m.app_version, '.', 1) AS INTEGER)              AS major,
    TRY_CAST(split_part(m.app_version, '.', 2) AS INTEGER)              AS minor,
    TRY_CAST(split_part(m.app_version, '.', 3) AS INTEGER)              AS patch

FROM default.all_ping_daily p

FULL JOIN gmeter_aggr m
    ON  p.device_id          = m.device_id
    AND p.local_created_date = m.local_created_date

LEFT JOIN default.all_gigameter_appversion_funnel a
    ON COALESCE(p.device_id, m.device_id) = a.device_id
;
