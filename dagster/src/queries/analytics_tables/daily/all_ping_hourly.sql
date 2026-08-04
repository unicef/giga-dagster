


-- ==============================================================================
-- Script Name:     all_ping_hourly.sql
-- Table Created:   default.all_ping_hourly
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Hourly aggregated ping/connectivity data ONLY (no GigaMeter speed test
--   measurements). Provides detailed hourly granularity for connectivity
--   analysis and uptime calculation without the overhead of GigaMeter data.
--
-- Source:
--   Derived from all_gigameter_inc_ping_prd.sql with GigaMeter
--   components removed for lightweight ping-only analysis.
--
-- Key Features:
--   - Hourly aggregation by school-device
--   - Simple is_school_hours boolean (TRUE for hours 8-20)
--   - Overall uptime calculation (connected pings / total pings)
--   - Lightweight structure optimized for ping-only analysis
--
-- Memory Optimization Options (uncomment if needed):
-- SET SESSION query_max_memory = '8GB';
-- SET SESSION query_max_total_memory_per_node = '8GB';
--
-- ==============================================================================



  CREATE TABLE IF NOT EXISTS default.all_ping_hourly
WITH (
    location = 'abfss://giga-dataops-prod@saunigiga.dfs.core.windows.net/warehouse/all_ping_hourly'
)
AS (


-- ==============================================================================
-- CTE: ping_base
-- Purpose: Extract and transform raw ping connectivity checks
-- Pre-compute local_hour here to avoid repeated EXTRACT calls
-- ==============================================================================




WITH school_lookup AS (

    SELECT
        school.giga_id_school AS school_id_giga,
        school.external_id AS school_id_govt,
        school.name AS school_name,
        country.name AS country,
        country.code AS iso2_code,
        -- RENAMED: iso3_format -> iso3_code for clarity
        UPPER(TRIM(country.iso3_format)) AS iso3_code,
        -- PRE-COMPUTED: Timezone with fallback, eliminates JOINs in final SELECT
        COALESCE(tz.timezone, tz_name.timezone, 'UTC') AS timezone,

        school.admin_1_name AS admin_1,
        school.admin_2_name AS admin_2,
        school.admin_3_name AS admin_3,
        school.admin_4_name AS admin_4
        -- select *
    FROM
        gigameter_production_db.public.school
    LEFT JOIN
        gigameter_production_db.public.country
        ON country.id = school.country_id
    -- Timezone lookup by ISO3 code (primary method)
    LEFT JOIN default.country_timezones tz
        ON country.iso3_format = tz.iso3
    -- Timezone lookup by country name (fallback method)
    LEFT JOIN default.country_timezones tz_name
        ON LOWER(country.name) = LOWER(tz_name.country)
    WHERE
        -- FILTER: Exclude deleted schools at source (was in final WHERE clause)
        school.deleted IS NULL
)






, ping_base AS (

    SELECT
      DISTINCT
        cpc.id AS connectivity_id,
        cpc.timestamp,
       -- CAST(at_timezone(cpc.timestamp, COALESCE(tz.timezone, 'UTC')) AS DATE) AS local_created_date,
       -- CAST(date_trunc('hour', at_timezone(cpc.timestamp, COALESCE(tz.timezone, 'UTC'))) AS TIMESTAMP) AS local_date_hour,
       -- EXTRACT(HOUR FROM at_timezone(cpc.timestamp, COALESCE(tz.timezone, 'UTC'))) AS local_hour,
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
      cpc.timestamp >= date_add('day', -90, current_timestamp)
)





, ping_enriched as (

SELECT

        CAST(at_timezone(pb.timestamp, sl.timezone) AS TIMESTAMP) AS local_ts,
    ---- PING DATA FIELDS
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
        -- RENAMED: iso3_format -> iso3_code for clarity
        sl.iso3_code,
        -- PRE-COMPUTED: Timezone with fallback, eliminates JOINs in final SELECT
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




-- ==============================================================================
-- CTE: ping_aggr
-- Purpose: Aggregate ping data by school-device-hour
-- ==============================================================================
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
        --COUNT(DISTINCT app_local_uuid) AS is_connected_all,
        AVG(latency) AS avg_latency,
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END) as is_connected_true,

        -- Uptime: ratio of connected pings to total pings
        SUM(CASE WHEN is_connected = TRUE THEN 1 ELSE 0 END)
            / CAST(COUNT(*) AS DECIMAL(10,2)) AS uptime,

        -- AVAILABILITY: ratio of signals recieved vs expected
        1 - ( (4 - COALESCE(COUNT(*), 0)) / cast(4 AS DECIMAL(10,2)) ) as availability

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



-- ==============================================================================
-- FINAL SELECT
-- Purpose: Output ping-only aggregated data with first ping validation
-- ==============================================================================
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
    --4 - COALESCE(p.ping_records, 0) AS missing_pings,
    CAST(p.missing_pings AS BIGINT) AS missing_pings,

    -- Ping metrics
    CAST(p.ping_records AS BIGINT) AS ping_records,
    --p.is_connected_all,
    CAST(p.is_connected_true AS BIGINT) AS is_connected_true,

    -- School hours indicator: TRUE for hours 8-20 (8am-8pm)
    CAST(p.local_hour BETWEEN 8 AND 20 AS BOOLEAN) AS is_school_hours,

    CAST(p.avg_latency AS DOUBLE) AS latency_ping,
    -- Uptime represents what proportion of ping signals received were connected
    CAST(p.uptime AS DOUBLE) AS uptime,
    -- availability gives a % of how many times device was available for ping
    CAST(p.availability AS DOUBLE) AS availability

FROM
  ping_aggr p

);
