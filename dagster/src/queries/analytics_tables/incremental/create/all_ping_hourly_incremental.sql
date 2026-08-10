
CREATE TABLE delta_lake.default.all_ping_hourly_incremental
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_ping_hourly_incremental'
)
AS (

-- =============================================================================
-- CTE: school_lookup
-- Purpose: Creates a reusable lookup for school metadata with pre-computed timezone
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
-- NO TIME FILTER: unlike the 90-day rolling window in the daily drop-and-recreate
-- version (all_ping_hourly.sql), this initial build reads all available raw ping
-- history so the incremental table starts with complete history, not a 90-day
-- slice of it. The update/ script re-filters going forward from there.
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
)


-- =============================================================================
-- CTE: ping_enriched
-- Purpose: Join raw pings to school metadata and compute the LOCAL timestamp
--
-- Computing local_ts here (rather than filtering ping_base on the raw UTC
-- cpc.timestamp first) means any future watermark/window filter in the update/
-- script can compare local-to-local against local_date_hour with no timezone
-- skew to account for.
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

 )


-- =============================================================================
-- CTE: ping_aggr
-- Purpose: Aggregate ping data by school-device-hour
--
-- connectivity_ids: the raw ping ids that fed into each bucket. Not part of the
-- daily version's schema — kept here so the update/ script can detect pings
-- that arrive late (after their bucket was already inserted) by comparing this
-- array against what's already stored, and correct that bucket via MERGE.
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


-- =============================================================================
-- FINAL SELECT
-- Purpose: Output ping-only aggregated data with first ping validation
-- =============================================================================
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
