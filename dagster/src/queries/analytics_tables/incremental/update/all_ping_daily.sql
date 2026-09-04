
-- =============================================================================
-- Forward-only watermark + composite-key dedup, not MERGE
--
-- Unlike all_ping_hourly, this table doesn't need the MERGE/
-- reconciliation treatment: a day is only ever rolled up once it's fully in the
-- past (local_created_date < CURRENT_DATE), which is a much larger natural
-- buffer than the hourly script's 4-hour grace period, and any hourly-bucket
-- correction that landed within that buffer is already reflected in
-- all_ping_hourly by the time this runs. Plain INSERT + anti-join
-- dedup is enough here.
-- =============================================================================

INSERT INTO delta_lake.default.all_ping_daily


-- =============================================================================
-- CTE: ping_daily
-- Purpose: Roll all_ping_hourly up from device-school-hour to
-- device-school-DAY, for days not yet in the target table.
--
-- Watermark: local_created_date >= MAX(local_created_date) already in the
-- target -- forward-only, no rolling cap, so no historical data is ever
-- dropped. The >= (not >) deliberately overlaps the last-inserted date, since
-- some schools' hourly data for that day may have settled after the previous
-- run; the anti-join dedup below handles the resulting overlap safely.
-- =============================================================================
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

        SUM(ping_records) AS ping_records,

        SUM(latency_ping * ping_records) / NULLIF(SUM(ping_records), 0) AS avg_latency,

        SUM(CASE WHEN local_hour BETWEEN 8 AND 19
                 THEN ping_records ELSE 0 END)                              AS pings_8am_to_8pm_local,

        SUM(CASE WHEN local_hour BETWEEN 8 AND 19
                 THEN is_connected_true ELSE 0 END)                         AS connected_8am_to_8pm_local,

        SUM(CASE WHEN local_hour BETWEEN 8 AND 19
                 THEN ping_records - is_connected_true ELSE 0 END)          AS not_connected_8am_to_8pm_local,

        SUM(CASE WHEN local_hour NOT BETWEEN 8 AND 19
                 THEN ping_records ELSE 0 END)                              AS invalid_ping_9pm_to_7am_local,

        SUM(CASE WHEN local_hour NOT BETWEEN 8 AND 19
                 THEN is_connected_true ELSE 0 END)                         AS connected_9pm_to_7am_local,

        SUM(CASE WHEN local_hour NOT BETWEEN 8 AND 19
                 THEN ping_records - is_connected_true ELSE 0 END)          AS not_connected_9pm_to_7am_local,

        SUM(is_connected_true)
            / NULLIF(CAST(SUM(ping_records) AS DECIMAL(10,2)), 0)           AS uptime,

        SUM(CASE WHEN local_hour BETWEEN 8 AND 19 THEN is_connected_true ELSE 0 END)
            / NULLIF(CAST(
                SUM(CASE WHEN local_hour BETWEEN 8 AND 19 THEN ping_records ELSE 0 END)
              AS DECIMAL(10,2)), 0)                                          AS uptime_8am_to_8pm_local

    FROM delta_lake.default.all_ping_hourly
    WHERE
        local_created_date >= (
            SELECT COALESCE(MAX(local_created_date), DATE '2022-01-01')
            FROM delta_lake.default.all_ping_daily
        )
        -- Only fully-complete days -- today is still accumulating hourly buckets
        AND local_created_date < CURRENT_DATE
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


first_ping_recorded AS (
    SELECT
        school_id_giga,
        MIN(local_created_date) AS first_ping_date
    FROM ping_daily
    GROUP BY school_id_giga
)


-- =============================================================================
-- FINAL SELECT
-- =============================================================================
SELECT
    CAST(p.local_created_date AS DATE)                          AS local_created_date,

    CAST(48 AS INTEGER)                                         AS expected_pings_per_day,
    CAST(48 - COALESCE(p.pings_8am_to_8pm_local, 0) AS BIGINT) AS missing_pings,

    CAST(p.device_id      AS VARCHAR)                           AS device_id,
    CAST(p.school_id_govt AS VARCHAR)                           AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR)                           AS school_id_giga,
    CAST(p.school_name    AS VARCHAR)                           AS school_name,
    CAST(p.country        AS VARCHAR)                           AS country,
    CAST(p.admin1         AS VARCHAR)                           AS admin1,
    CAST(p.admin2         AS VARCHAR)                           AS admin2,

    CAST(p.ping_records                   AS BIGINT)            AS ping_records,
    CAST(p.pings_8am_to_8pm_local         AS BIGINT)            AS pings_8am_to_8pm_local,
    CAST(p.connected_8am_to_8pm_local     AS BIGINT)            AS connected_8am_to_8pm_local,
    CAST(p.not_connected_8am_to_8pm_local AS BIGINT)            AS not_connected_8am_to_8pm_local,
    CAST(p.invalid_ping_9pm_to_7am_local  AS BIGINT)            AS invalid_ping_9pm_to_7am_local,
    CAST(p.connected_9pm_to_7am_local     AS BIGINT)            AS connected_9pm_to_7am_local,
    CAST(p.not_connected_9pm_to_7am_local AS BIGINT)            AS not_connected_9pm_to_7am_local,

    CAST(p.avg_latency            AS DOUBLE)                    AS latency_ping,
    CAST(p.uptime                 AS DOUBLE)                    AS uptime,
    CAST(p.uptime_8am_to_8pm_local AS DOUBLE)                   AS uptime_8am_to_8pm_local

FROM ping_daily p
JOIN first_ping_recorded f
    ON p.school_id_giga = f.school_id_giga
WHERE p.local_created_date >= f.first_ping_date
  AND p.local_created_date <= CURRENT_DATE
  -- Dedup: skip any (date, school, device) already present -- guards the
  -- boundary day the watermark deliberately overlaps
  AND NOT EXISTS (
      SELECT 1
      FROM delta_lake.default.all_ping_daily t
      WHERE t.local_created_date = CAST(p.local_created_date AS DATE)
        AND t.school_id_giga = CAST(p.school_id_giga AS VARCHAR)
        AND t.device_id = CAST(p.device_id AS VARCHAR)
  )
