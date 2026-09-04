
-- =============================================================================
-- MERGE, not INSERT
--
-- This table is a GROUP BY aggregation (one row per school+device+hour), unlike
-- the other 4 incremental scripts which are flat, per-measurement rows. A plain
-- INSERT can't safely correct an already-written bucket without duplicating its
-- key and double-counting downstream (all_ping_daily sums
-- ping_records by day off this table) -- so late-arriving pings are handled via
-- MERGE: insert new (settled) buckets, replace a bucket in place if reconciliation
-- finds its raw ping ids changed since it was last written.
--
-- Two safeguards against losing/missing pings near an hour boundary:
--   1. 4-hour grace period -- a bucket is only INSERTed once local_date_hour is
--      at least 4 hours old, giving most stragglers time to land in the source
--      table before that bucket is ever finalized.
--   2. connectivity_ids reconciliation -- every run re-aggregates a 48h trailing
--      window and compares each bucket's freshly computed connectivity_ids
--      against what's already stored. Any bucket whose id set grew (a genuinely
--      late ping, past the grace period) gets its whole row replaced with the
--      corrected aggregate.
-- =============================================================================

MERGE INTO delta_lake.default.all_ping_hourly AS target
USING (


-- =============================================================================
-- CTE: school_lookup
-- Purpose: Creates a reusable lookup for school metadata with pre-computed timezone
-- =============================================================================
WITH school_lookup AS (

    SELECT
        school.giga_id_school AS school_id_giga,
        school.external_id AS school_id_govt,
        school.name AS school_name,
        country.name AS country,
        country.code AS iso2_code,
        UPPER(TRIM(country.iso3_format)) AS iso3_code,
        COALESCE(tz.timezone, tz_name.timezone, 'UTC') AS timezone,

        school.admin_1_name AS admin_1,
        school.admin_2_name AS admin_2,
        school.admin_3_name AS admin_3,
        school.admin_4_name AS admin_4
    FROM
        gigameter_production_db.public.school
    LEFT JOIN
        gigameter_production_db.public.country
        ON country.id = school.country_id
    LEFT JOIN default.country_timezones tz
        ON country.iso3_format = tz.iso3
    LEFT JOIN default.country_timezones tz_name
        ON LOWER(country.name) = LOWER(tz_name.country)
    WHERE
        school.deleted IS NULL
)


-- =============================================================================
-- CTE: ping_base
-- Purpose: Extract raw ping connectivity checks
--
-- WHERE clause here is a coarse, partition-friendly PERFORMANCE pre-filter on
-- the raw UTC timestamp only -- deliberately wider (72h) than the 48h
-- reconciliation window below, to safely absorb the local-time <-> UTC offset
-- (up to ~14h) without risking clipping a row before the precise filter in
-- ping_enriched gets to see it. This is NOT the correctness boundary.
-- =============================================================================
, ping_base AS (

    SELECT
      DISTINCT
        cpc.id AS connectivity_id,
        cpc.timestamp,
        cpc.is_connected,
        cpc.error_message,
        cpc.giga_id_school AS school_id_giga,
        cpc.app_local_uuid,
        cpc.browser_id AS device_id,
        cpc.created_at AS connectivity_created_at,
        cpc.latency
    FROM
      gigameter_production_db.public.connectivity_ping_checks cpc
    WHERE
      cpc.timestamp >= (
          SELECT COALESCE(MAX(local_date_hour), TIMESTAMP '2022-01-01 00:00:00')
          FROM delta_lake.default.all_ping_hourly
      ) - INTERVAL '72' HOUR
)


-- =============================================================================
-- CTE: ping_enriched
-- Purpose: Join raw pings to school metadata, compute local_ts, and apply the
-- PRECISE reconciliation window filter -- local-to-local, so no timezone buffer
-- is needed here (unlike the coarse raw-UTC pre-filter in ping_base above).
-- =============================================================================
, ping_enriched as (

SELECT

        CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) AS local_ts,
    ---- PING DATA FIELDS
        pb.connectivity_id,
        pb.is_connected,
        pb.error_message,
        pb.app_local_uuid,
        pb.device_id,
        pb.connectivity_created_at,
        pb.latency,
    ---- SCHOOL DATA FIELDS
        sl.school_id_giga,
        sl.school_id_govt,
        sl.school_name,
        sl.country,
        sl.iso2_code,
        sl.iso3_code,
        sl.timezone,

        sl.admin_1,
        sl.admin_2,
        sl.admin_3,
        sl.admin_4

FROM
  ping_base pb
LEFT JOIN
  school_lookup sl
ON
  pb.school_id_giga = sl.school_id_giga

WHERE
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= (
      SELECT COALESCE(MAX(local_date_hour), TIMESTAMP '2022-01-01 00:00:00')
      FROM delta_lake.default.all_ping_hourly
  ) - INTERVAL '48' HOUR

 )


-- =============================================================================
-- CTE: ping_aggr
-- Purpose: Aggregate ping data by school-device-hour, same as create/
-- =============================================================================
 , ping_aggr AS (


    SELECT
        CAST(local_ts AS DATE) AS local_created_date,
        CAST(date_trunc('hour', local_ts) AS TIMESTAMP) AS local_date_hour,
        EXTRACT(HOUR FROM local_ts) AS local_hour,
        country,
        school_id_giga,
        school_id_govt,
        school_name,
        admin_1,
        admin_2,
        admin_3,
        admin_4,
        device_id,
        -- Carried through so the grace period below can be evaluated in each
        -- school's own local time, not the session's UTC "now"
        timezone,

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Sorted for stable equality comparison against target.connectivity_ids
        ARRAY_SORT(ARRAY_AGG(DISTINCT connectivity_id)) AS connectivity_ids

    FROM
      ping_enriched

    GROUP BY
        CAST(local_ts AS DATE),
        CAST(date_trunc('hour', local_ts) AS TIMESTAMP),
        EXTRACT(HOUR FROM local_ts),
        country,
        school_id_giga,
        school_id_govt,
        school_name,
        admin_1,
        admin_2,
        admin_3,
        admin_4,
        device_id,
        timezone

)


-- =============================================================================
-- FINAL SELECT (this becomes the MERGE's `source`)
-- =============================================================================
SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    CAST(p.uptime AS DOUBLE) AS uptime,
    CAST(p.availability AS DOUBLE) AS availability,

    p.connectivity_ids,

    -- Control column for the MERGE's grace-period check below -- NOT part of the
    -- target table, excluded from the INSERT column list. Evaluated in the
    -- school's own local time (via p.timezone), not session "now", for the same
    -- reason the reconciliation window above compares local-to-local.
    CAST(
        p.local_date_hour <= CAST(at_timezone(current_timestamp, p.timezone) AS TIMESTAMP) - INTERVAL '4' HOUR
    AS BOOLEAN) AS is_settled

FROM
  ping_aggr p


) AS source
ON
    target.local_date_hour = source.local_date_hour
    AND target.school_id_giga = source.school_id_giga
    AND target.device_id = source.device_id

-- Late-arriving pings: the bucket already exists but gained new raw ids since it
-- was last written -- replace it with the corrected aggregate.
WHEN MATCHED AND target.connectivity_ids != source.connectivity_ids THEN
    UPDATE SET
        local_created_date = source.local_created_date,
        local_date_minus_1hr = source.local_date_minus_1hr,
        local_hour = source.local_hour,
        school_name = source.school_name,
        country = source.country,
        admin1 = source.admin1,
        admin2 = source.admin2,
        expected_pings_per_hour = source.expected_pings_per_hour,
        missing_pings = source.missing_pings,
        ping_records = source.ping_records,
        is_connected_true = source.is_connected_true,
        is_school_hours = source.is_school_hours,
        latency_ping = source.latency_ping,
        uptime = source.uptime,
        availability = source.availability,
        connectivity_ids = source.connectivity_ids

-- New bucket, only once it's past the 4-hour grace period (buckets still inside
-- the window are left for a later run rather than finalized early).
WHEN NOT MATCHED AND source.is_settled THEN
    INSERT (
        local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
        device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
        expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
        is_school_hours, latency_ping, uptime, availability, connectivity_ids
    )
    VALUES (
        source.local_created_date, source.local_date_hour, source.local_date_minus_1hr, source.local_hour,
        source.device_id, source.school_id_govt, source.school_id_giga, source.school_name, source.country, source.admin1, source.admin2,
        source.expected_pings_per_hour, source.missing_pings, source.ping_records, source.is_connected_true,
        source.is_school_hours, source.latency_ping, source.uptime, source.availability, source.connectivity_ids
    )
