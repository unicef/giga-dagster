-- ==============================================================================
-- Script Name:     all_ping_daily.sql
-- Table Created:   default.all_ping_daily
-- Schema:          default
-- Pipeline Step:   1 of 2 (ping aggregation only)
--
-- Purpose:
--   Daily aggregated ping/connectivity data per school-device. This is Step 1
--   of a two-step replacement for all_gigameter_inc_ping_daily.sql, split to
--   reduce memory pressure from the original single-query approach.
--
-- Source:          default.all_ping_hourly
-- Why this source:
--   all_ping_hourly groups raw pings by device-school-HOUR, so each row
--   represents exactly one hour (local_hour = 0..23). This means ping_records
--   and is_connected_true on each row are raw counts for that specific hour.
--   To get daily totals we SUM these counts — sum-of-hourly-sums equals the
--   full raw daily count. To split school-hours (8am–8pm) vs off-hours we use
--   CASE WHEN local_hour BETWEEN 8 AND 19, which is valid precisely because
--   each row belongs to one and only one hour.
--
-- Derived From:    all_gigameter_inc_ping_daily.sql (ping CTEs only)
-- Key Differences:
--   - Reads all_ping_hourly (pre-aggregated) instead of raw connectivity_ping_checks
--   - No timezone JOINs, school JOINs — already resolved in all_ping_hourly
--   - No COUNT DISTINCT (is_connected_all dropped; cannot reconstruct from hourly)
--   - Rolling window inherited from all_ping_hourly (90 days)
--   - Weighted average latency: SUM(latency * pings) / SUM(pings)
--
-- Pipeline order:
--   all_ping_hourly  →  all_ping_daily  →  all_gigameter_inc_ping_daily
-- ==============================================================================


-- DROP TABLE IF EXISTS default.all_ping_daily;

 CREATE TABLE default.all_ping_daily
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_ping_daily',
    partitioned_by = ARRAY['country']
)
AS


-- ==============================================================================
-- CTE: ping_daily
-- Purpose: Roll all_ping_hourly up from device-school-hour to device-school-DAY
--
-- Time-window split logic:
--   Each row in all_ping_hourly has a single local_hour value (0–23).
--   ALL ping_records in a row with local_hour=9 are hour-9 pings (school hours).
--   ALL ping_records in a row with local_hour=22 are off-hours pings.
--   Therefore CASE WHEN local_hour BETWEEN 8 AND 19 THEN ping_records gives the
--   exact count of pings in that window — no ambiguity, no approximation.
--
-- Latency:
--   latency_ping in all_ping_hourly is AVG(latency) per hour. A plain AVG across
--   hours would be an average-of-averages, wrong when hours have different counts.
--   Weighted average: SUM(latency * ping_records) / SUM(ping_records) is correct.
-- ==============================================================================
WITH ping_daily AS (
    SELECT
        local_created_date,
        country,
        school_id_giga,
        school_id_govt,
        school_name,
        admin1,
        admin2,
        device_id,

        -- Total pings for the day
        SUM(ping_records) AS ping_records,

        -- Weighted average latency across all hours
        SUM(latency_ping * ping_records) / NULLIF(SUM(ping_records), 0) AS avg_latency,

        -- School hours (8am–8pm = hours 8–19)
        -- ping_records on each hourly row are all within that row's local_hour,
        -- so summing rows where local_hour BETWEEN 8 AND 19 gives the exact school-hours total.
        SUM(CASE WHEN local_hour BETWEEN 8 AND 19
                 THEN ping_records ELSE 0 END)                              AS pings_8am_to_8pm_local,

        SUM(CASE WHEN local_hour BETWEEN 8 AND 19
                 THEN is_connected_true ELSE 0 END)                         AS connected_8am_to_8pm_local,

        SUM(CASE WHEN local_hour BETWEEN 8 AND 19
                 THEN ping_records - is_connected_true ELSE 0 END)          AS not_connected_8am_to_8pm_local,

        -- Off-hours (8pm–8am = hours 0–7 and 20–23)
        SUM(CASE WHEN local_hour NOT BETWEEN 8 AND 19
                 THEN ping_records ELSE 0 END)                              AS invalid_ping_9pm_to_7am_local,

        SUM(CASE WHEN local_hour NOT BETWEEN 8 AND 19
                 THEN is_connected_true ELSE 0 END)                         AS connected_9pm_to_7am_local,

        SUM(CASE WHEN local_hour NOT BETWEEN 8 AND 19
                 THEN ping_records - is_connected_true ELSE 0 END)          AS not_connected_9pm_to_7am_local,

        -- Overall daily uptime: total connected / total pings
        SUM(is_connected_true)
            / NULLIF(CAST(SUM(ping_records) AS DECIMAL(10,2)), 0)           AS uptime,

        -- School-hours uptime: connected during 8am–8pm / total pings during 8am–8pm
        SUM(CASE WHEN local_hour BETWEEN 8 AND 19 THEN is_connected_true ELSE 0 END)
            / NULLIF(CAST(
                SUM(CASE WHEN local_hour BETWEEN 8 AND 19 THEN ping_records ELSE 0 END)
              AS DECIMAL(10,2)), 0)                                          AS uptime_8am_to_8pm_local

    FROM default.all_ping_hourly
    GROUP BY
        local_created_date,
        country,
        school_id_giga,
        school_id_govt,
        school_name,
        admin1,
        admin2,
        device_id
),


-- ==============================================================================
-- CTE: first_ping_recorded
-- Purpose: Earliest ping date per school within the rolling window.
--          Used to exclude dates before a school's first ping.
-- ==============================================================================
first_ping_recorded AS (
    SELECT
        school_id_giga,
        MIN(local_created_date) AS first_ping_date
    FROM ping_daily
    GROUP BY school_id_giga
)


-- ==============================================================================
-- FINAL SELECT
-- ==============================================================================
SELECT
    CAST(p.local_created_date AS DATE)                          AS local_created_date,

    -- Ping completeness (48 expected = 12 school hours × 4 pings/hour)
    CAST(48 AS INTEGER)                                         AS expected_pings_per_day,
    CAST(48 - COALESCE(p.pings_8am_to_8pm_local, 0) AS BIGINT) AS missing_pings,

    -- Identifiers
    CAST(p.device_id      AS VARCHAR)                           AS device_id,
    CAST(p.school_id_govt AS VARCHAR)                           AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR)                           AS school_id_giga,
    CAST(p.school_name    AS VARCHAR)                           AS school_name,
    CAST(p.country        AS VARCHAR)                           AS country,
    CAST(p.admin1         AS VARCHAR)                           AS admin1,
    CAST(p.admin2         AS VARCHAR)                           AS admin2,

    -- Ping totals
    CAST(p.ping_records                   AS BIGINT)            AS ping_records,
    CAST(p.pings_8am_to_8pm_local         AS BIGINT)            AS pings_8am_to_8pm_local,
    CAST(p.connected_8am_to_8pm_local     AS BIGINT)            AS connected_8am_to_8pm_local,
    CAST(p.not_connected_8am_to_8pm_local AS BIGINT)            AS not_connected_8am_to_8pm_local,
    CAST(p.invalid_ping_9pm_to_7am_local  AS BIGINT)            AS invalid_ping_9pm_to_7am_local,
    CAST(p.connected_9pm_to_7am_local     AS BIGINT)            AS connected_9pm_to_7am_local,
    CAST(p.not_connected_9pm_to_7am_local AS BIGINT)            AS not_connected_9pm_to_7am_local,

    -- Latency and uptime
    CAST(p.avg_latency            AS DOUBLE)                    AS latency_ping,
    CAST(p.uptime                 AS DOUBLE)                    AS uptime,
    CAST(p.uptime_8am_to_8pm_local AS DOUBLE)                   AS uptime_8am_to_8pm_local

FROM ping_daily p
JOIN first_ping_recorded f
    ON p.school_id_giga = f.school_id_giga
WHERE p.local_created_date >= f.first_ping_date
  AND p.local_created_date <= CURRENT_DATE
 ;
