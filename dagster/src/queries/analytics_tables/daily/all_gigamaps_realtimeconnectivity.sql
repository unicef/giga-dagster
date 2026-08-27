


-- ==============================================================================
-- Script Name:     all_gigamaps_realtimeconnectivity.sql
-- Table Created:   default.all_gigamaps_realtimeconnectivity
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Daily snapshot of school connectivity status aggregated by country and date,
--   sourced from the GigaMaps real-time monitoring registry. Tracks the number
--   of mapped schools, connected schools (good/moderate status), and schools
--   registered for real-time monitoring over time.
--
-- Dependencies:
--   - gigamaps_production_db.public.schools (school registry with created date)
--   - gigamaps_production_db.public.school_realtime_registrations (RT registrations)
--
-- Output Columns:  ~6 columns
-- Primary Key:     country_code + date
-- Granularity:     One row per country per day
--
-- Run Notes:
--   Recurring — refresh daily. Uses a FULL OUTER JOIN across three data sources
--   to ensure all schools are captured regardless of connectivity status.
--
-- Last Updated:    2025-10-31 / Luke Stringer
-- ==============================================================================


 DROP TABLE IF EXISTS default.all_gigamaps_realtimeconnectivity;

 CREATE TABLE IF NOT EXISTS  default.all_gigamaps_realtimeconnectivity
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigamaps_realtimeconnectivity'
)
AS (



-- CTE for total schools mapped
WITH mapped_schools_count AS (


    SELECT
    MIN(CAST(DATE_TRUNC('day', s.created) AS DATE)) AS schools_count_date,
    c.name AS country,
    s.id as giga_schools
    FROM gigamaps_production_db.public.schools_school s
     LEFT JOIN gigamaps_production_db.public.locations_country c ON s.country_id = c.id
    WHERE s.deleted IS NULL
    GROUP BY c.name, s.id


),

-- SELECT count(DISTINCT giga_schools) as total_mapped_schools from mapped_schools_count



-- CTE for connected schools
connected_schools AS (


    SELECT

    MIN(CAST(DATE_TRUNC('day', s.created) AS DATE)) AS connectivity_date,
    c.name AS country,
    s.id AS giga_schools
    FROM gigamaps_production_db.public.schools_school s
     LEFT JOIN gigamaps_production_db.public.locations_country c ON s.country_id = c.id
    WHERE s.deleted IS NULL
      AND s.connectivity_status IN ('good', 'moderate')
    GROUP BY c.name, s.id

),

-- SELECT count(DISTINCT giga_schools) as connected from connected_schools


-- CTE for not connected schools
not_connected_cte AS (


    SELECT
    c.name as country,
    COUNT(DISTINCT s.id) AS not_connected
    FROM gigamaps_production_db.public.schools_school s
    LEFT JOIN gigamaps_production_db.public.locations_country c ON s.country_id = c.id
    WHERE s.deleted IS NULL
      AND s.connectivity_status = 'no'
    GROUP BY c.name

),


-- CTE for unknown connectivity status
unknown_cte AS (

    SELECT
    c.name as country,
    COUNT(DISTINCT s.id) AS unknown
    FROM gigamaps_production_db.public.schools_school s
    LEFT JOIN gigamaps_production_db.public.locations_country c ON s.country_id = c.id
    WHERE s.deleted IS NULL
      AND s.connectivity_status = 'unknown'
     GROUP BY c.name
),




latest_week AS (
    SELECT
         EXTRACT(week FROM CURRENT_DATE) AS current_week,
         EXTRACT(year FROM CURRENT_DATE) AS current_year
),


schools_w_rt_data AS (
    SELECT
        MIN(CAST(DATE_TRUNC('day', dailyconn_stats.created) AS DATE)) AS realtime_connectivity_date,
        s.id AS giga_schools,
        c.name AS country
    FROM gigamaps_production_db.public.schools_school s
    INNER JOIN gigamaps_production_db.public.connection_statistics_schoolrealtimeregistration r
        ON s.id = r.school_id
    LEFT OUTER JOIN gigamaps_production_db.public.connection_statistics_schoolweeklystatus w
        ON s.id = w.school_id
          AND w.year = (SELECT current_year FROM latest_week)
          AND w.week = (SELECT current_week - 1 FROM latest_week)
          AND w.deleted IS NULL
    LEFT JOIN gigamaps_production_db.public.connection_statistics_schooldailystatus dailyconn_stats
        ON s.id = dailyconn_stats.school_id
    INNER JOIN gigamaps_production_db.public.locations_country c
        ON s.country_id = c.id
    WHERE s.deleted IS NULL
      AND r.deleted IS NULL
      AND r.rt_registered = TRUE
      AND CAST(r.rt_registration_date AT TIME ZONE 'UTC' AS DATE) <= date_add('day', -1, date_trunc('week', CURRENT_DATE))
    GROUP BY s.id, c.name
    ORDER BY MIN(CAST(DATE_TRUNC('day', dailyconn_stats.created) AS DATE)) DESC
),

--SELECT COUNT(DISTINCT giga_schools) FROM schools_w_rt_data



-------------------------------------------------------------------------------------------------------------------------------------------------------------


mapped_schools as (

    select

              country,
              schools_count_date,
              count(distinct giga_schools) as schools_added
              from mapped_schools_count
              group by 1, 2


),

connected_giga_id_schools AS (
    SELECT
         country,
         connectivity_date,

        COUNT(DISTINCT giga_schools) AS connected_schools

    FROM connected_schools
    GROUP BY 1, 2
),


real_time_connectivity_status as (

            select

              country,
              realtime_connectivity_date,
              count(distinct giga_schools) as count_realtime_mapped_schools
            from
              schools_w_rt_data
              group by 1,2

      )


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- cs.connectivity_date




SELECT
      CAST(COALESCE(realtimeconn.realtime_connectivity_date, ms.schools_count_date, cs.connectivity_date) AS DATE) AS date,
       CAST(COALESCE(ms.country, realtimeconn.country, cs.country) AS VARCHAR) AS country,
      CAST(COALESCE(schools_added, 0) AS BIGINT) as schools_added,
      CAST(COALESCE(cs.connected_schools, 0) AS BIGINT) as connected_schools,
      CAST(COALESCE(count_realtime_mapped_schools, 0) AS BIGINT) as count_realtime_mapped_schools

FROM mapped_schools ms
LEFT JOIN connected_giga_id_schools cs
         ON ms.schools_count_date = cs.connectivity_date AND ms.country = cs.country
FULL OUTER  JOIN real_time_connectivity_status realtimeconn
        ON COALESCE(ms.schools_count_date, realtimeconn.realtime_connectivity_date) = realtimeconn.realtime_connectivity_date AND  cs.country = realtimeconn.country

ORDER BY date DESC, country




   )
