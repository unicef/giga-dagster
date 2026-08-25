-- =============================================================================
-- BOOTSTRAP: chunked historical backfill
--
-- A full-history GROUP BY in one query exceeds Trino's per-node memory limit
-- (~29M raw rows, 2026-08-19). A raw-row LIMIT isn't safe: create/ runs once
-- only (Dagster switches to update/'s MERGE once the table exists), and has
-- no MERGE to fix a bucket split by an arbitrary cutoff.
--
-- Fix: one CREATE + nine INSERTs, each bounded to an hour-aligned local-time
-- window using update/'s two-tier filter (wide raw-UTC buffer in ping_base,
-- precise local-to-local boundary in ping_enriched) -- since
-- local_date_hour = date_trunc('hour', local_ts), no bucket can be split
-- across chunks. Boundaries sized from actual 2026-08-19 row distribution
-- (volume is back-loaded into 2026, so widths aren't uniform). Chunk 1's
-- floor (2018-01-01) excludes ~7.5K rows with implausible timestamps (1980,
-- year 8663). Chunk 10 stops 2026-08-15; update/'s hourly MERGE closes the
-- rest on its own watermark.
-- =============================================================================


-- CHUNK 1 of 10: 2018-01-01 00:00:00 -> 2025-12-31 00:00:00  (bootstrap CREATE)
CREATE TABLE delta_lake.default.all_ping_hourly_incremental
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_ping_hourly_incremental',
    partitioned_by = ARRAY['country']
)
AS (

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
      cpc.timestamp >= TIMESTAMP '2018-01-01 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2025-12-31 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2018-01-01 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2025-12-31 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

)
;

-- CHUNK 2 of 10: 2025-12-31 00:00:00 -> 2026-01-30 00:00:00
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2025-12-31 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-01-30 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2025-12-31 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-01-30 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;

-- CHUNK 3 of 10: 2026-01-30 00:00:00 -> 2026-02-14 00:00:00
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2026-01-30 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-02-14 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2026-01-30 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-02-14 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;

-- CHUNK 4 of 10: 2026-02-14 00:00:00 -> 2026-03-03 00:00:00
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2026-02-14 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-03-03 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2026-02-14 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-03-03 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;

-- CHUNK 5 of 10: 2026-03-03 00:00:00 -> 2026-03-19 00:00:00
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2026-03-03 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-03-19 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2026-03-03 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-03-19 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;

-- CHUNK 6 of 10: 2026-03-19 00:00:00 -> 2026-04-10 00:00:00
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2026-03-19 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-04-10 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2026-03-19 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-04-10 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;

-- CHUNK 7 of 10: 2026-04-10 00:00:00 -> 2026-04-28 00:00:00
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2026-04-10 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-04-28 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2026-04-10 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-04-28 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;

-- CHUNK 8 of 10: 2026-04-28 00:00:00 -> 2026-05-15 00:00:00
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2026-04-28 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-05-15 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2026-04-28 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-05-15 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;

-- CHUNK 9 of 10: 2026-05-15 00:00:00 -> 2026-06-27 00:00:00
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2026-05-15 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-06-27 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2026-05-15 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-06-27 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;

-- CHUNK 10 of 10: 2026-06-27 00:00:00 -> 2026-08-15 00:00:00  (stop point -- update/ takes the tail from here)
INSERT INTO delta_lake.default.all_ping_hourly_incremental
(
    local_created_date, local_date_hour, local_date_minus_1hr, local_hour,
    device_id, school_id_govt, school_id_giga, school_name, country, admin1, admin2,
    expected_pings_per_hour, missing_pings, ping_records, is_connected_true,
    is_school_hours, latency_ping, uptime, availability, connectivity_ids
)
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
      cpc.timestamp >= TIMESTAMP '2026-06-27 00:00:00' - INTERVAL '72' HOUR
      AND cpc.timestamp <  TIMESTAMP '2026-08-15 00:00:00' + INTERVAL '72' HOUR
)
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
  CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) >= TIMESTAMP '2026-06-27 00:00:00'
  AND CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) <  TIMESTAMP '2026-08-15 00:00:00'

 )
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

        COUNT(*) AS ping_records,
        4 - COALESCE(COUNT(*), 0) AS missing_pings,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability,

        -- Raw ping ids in this bucket, sorted for stable equality comparison in the
        -- update/ script's MERGE (ARRAY_AGG order is not otherwise guaranteed)
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
        device_id

)


SELECT
  DISTINCT
    CAST(p.local_created_date AS DATE) AS local_created_date,
    CAST(p.local_date_hour AS TIMESTAMP) AS local_date_hour,

    -- Hour offset for timezone alignment
    CAST(date_add('hour', -1, p.local_date_hour) AS TIMESTAMP) AS local_date_minus_1hr,
    CAST(p.local_hour AS BIGINT) AS local_hour,

    -- Device and school identifiers
    CAST(p.device_id AS VARCHAR) AS device_id,
    CAST(p.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(p.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(p.school_name AS VARCHAR) AS school_name,
    CAST(p.country AS VARCHAR) AS country,
    CAST(p.admin_1 AS VARCHAR) AS admin1,
    CAST(p.admin_2 AS VARCHAR) AS admin2,

    -- Ping completeness metrics (4 expected: 1 ping every 15 minutes)
    CAST(4 AS BIGINT) AS expected_pings_per_hour,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability,

    -- Reconciliation tracking column - see ping_aggr comment above
    p.connectivity_ids

FROM
  ping_aggr p

;
