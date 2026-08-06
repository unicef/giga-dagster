






-- ==============================================================================
-- Script Name:     all_gigameter_registered_schools.sql
-- Table Created:   default.all_gigameter_registered_schools
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   School-level summary of GigaMeter registration and measurement activity.
--   Each row represents one school and captures registration status, measurement
--   sending status, total measurement counts, connectivity status
--   (live / at-risk / drop-off), and the most frequently observed ISP and server.
--
-- Dependencies:
--   - gigameter_production_db.public.school (GigaMeter school registrations)
--   - default.all_gigameter_measurement_data (consolidated measurement history)
--   - default.all_school_master (school metadata and geography)
--
-- Output Columns:  ~30 columns
-- Primary Key:     school_id_giga
-- Granularity:     One row per registered school
--
-- Run Notes:
--   Recurring — refresh daily or on-demand to reflect latest activity.
--   Connectivity status thresholds: live ≤21 days since last measurement,
--   at-risk 21–30 days, drop-off >30 days.
--
-- Last Updated:    2025-10-31 / Luke Stringer
-- ==============================================================================

CREATE TABLE IF NOT EXISTS default.all_gigameter_registered_schools
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_registered_schools'
)
as (

with


last_measurement_date as (
SELECT
  DISTINCT
    school_id_govt,
    school_id_giga,
    last_app_version,
    last_measurement_date
   -- rt_source
FROM
    (
        SELECT
            school_id_govt,
            school_id_giga,
            app_version AS last_app_version,
            created_timestamp AS last_measurement_date,
            ROW_NUMBER() OVER (PARTITION BY school_id_giga ORDER BY created_timestamp DESC) AS row_num
          --  rt_source
        FROM
            default.all_gigameter_measurement_data
        WHERE
          measurement_time_window = '8am-4pm(within school hrs)'
        AND
          is_weekday = TRUE
    ) AS a
WHERE
    row_num = 1
)


-- get the highest app version for a school
, max_app_version_per_school AS (
    SELECT
        school_id_giga,
        rt_source,
        app_version AS max_app_version
    FROM (
        SELECT
            school_id_giga,
            rt_source,
            app_version,
            ROW_NUMBER() OVER (
                PARTITION BY rt_source, school_id_giga
                ORDER BY
                    TRY_CAST(SPLIT_PART(app_version, '.', 1) AS INTEGER) DESC,
                    TRY_CAST(SPLIT_PART(app_version, '.', 2) AS INTEGER) DESC,
                    TRY_CAST(SPLIT_PART(app_version, '.', 3) AS INTEGER) DESC
            ) AS version_rank
        FROM default.all_gigameter_measurement_data
        WHERE app_version IS NOT NULL
    )
    WHERE version_rank = 1
)


-- num times registered
, registration_attempts AS (
    SELECT
        giga_id_school AS school_id_giga,
        COUNT(DISTINCT mac_address) AS num_mac_addressess,
        COUNT(DISTINCT created) AS num_registration_attempts,
        COUNT(DISTINCT user_id) AS num_devices_registered,
        MIN(created_at) as first_registration_attempt,
        MAX(created_at) as latest_registration_attempt,
        SUM(CASE WHEN is_active = FALSE then 0 else 1 end) as devices_still_logged_in        -- inverse count of the number of devices logged out to get devices still logged in
        --COUNT(is_active),
        --'DailyCheckApp' as rt_source
        -- select *
    FROM
      gigameter_production_db.public.dailycheckapp_school
    GROUP BY
      giga_id_school
)


-- identify if the last registration attempt remained logged in
-- may not be neccessary
, is_logged_in_latest_registration as (
  SELECT
    max(created_at) as latest_registration_attempt,
    giga_id_school as school_id_giga,
    is_active
  FROM
    gigameter_production_db.public.dailycheckapp_school
  GROUP BY
    giga_id_school, is_active
)


-- get data from measurement table
, measurement_data as (
SELECT
  DISTINCT
    m.country,
    m.iso3_code,
    m.school_id_giga,
    m.school_id_govt,
    --m.primary_source,
    --m.secondary_source,
    count(*) as num_measurements,
    count(DISTINCT device_id) as num_devices_measured,
    approx_most_frequent(5, m.rt_source, 5) as rt_source_nested,
    approx_most_frequent(5, m.detected_server, 5) as detected_server_nested ,
    approx_most_frequent(5, m.isp_name, 5) as detected_isp_nested,
    approx_most_frequent(5, m.isp_asn, 5) as detected_isp_asn_nested,
    min(m.local_created_timestamp) as first_measurement_date,
    count(distinct date_trunc('day', m.local_created_timestamp)) AS num_days_measured,                        -- number of days measurements have been received
    date_diff('day', CAST(MIN(m.local_created_timestamp) AS DATE), current_date) as num_days_measuring        -- number of days since starting to send measurement data
    -- select *
FROM
  default.all_gigameter_measurement_data m
WHERE
  measurement_time_window = '8am-4pm(within school hrs)'            -- filter data for those inside measurement window only
AND
  is_weekday = TRUE
group by
    m.country,
    m.iso3_code,
    m.school_id_giga,
    m.school_id_govt
)


-- final query
SELECT
  CAST(master.country AS VARCHAR) AS country,
  CAST(master.iso3_code AS VARCHAR) AS iso3_code,
  CAST(master.school_id_giga AS VARCHAR) AS school_id_giga,
  CAST(master.school_name AS VARCHAR) AS school_name,
  CAST(master.school_id_govt AS VARCHAR) AS school_id_govt,
  -- registration info
  CAST(case when r.first_registration_attempt is not null then 'Yes' else 'No' end AS VARCHAR) AS registered_gigameter,
  CAST(case when data.first_measurement_date is not null then 'Yes' else 'No' end AS VARCHAR) AS sending_gigameter_data,
  CAST(data.first_measurement_date AS TIMESTAMP) AS first_measurement_date,
  CAST(lm.last_measurement_date AS TIMESTAMP) AS last_measurement_date,
  CAST(EXTRACT(DAY FROM CURRENT_DATE - lm.last_measurement_date) AS DOUBLE) AS days_since_last_measurement,
  -- rt sources (MAP — pass through)
  data.rt_source_nested as measurements_per_source,
  -- devices
  CAST(r.num_devices_registered AS BIGINT) AS num_devices_registered,
  CAST(r.devices_still_logged_in AS BIGINT) AS devices_still_logged_in,
  CAST(data.num_devices_measured AS BIGINT) AS num_devices_measured,
  CAST(app.max_app_version AS VARCHAR) AS max_app_version_gigameter,
  -- provider info (MAP — pass through)
  data.detected_isp_nested,
  data.detected_isp_asn_nested,
  data.detected_server_nested,
  -- additional geography
  CAST(master.admin1 AS VARCHAR) AS admin1,
  CAST(master.admin2 AS VARCHAR) AS admin2,
  CAST(master.unicef_region AS VARCHAR) AS unicef_region,
  CAST(master.latitude AS DOUBLE) AS latitude,
  CAST(master.longitude AS DOUBLE) AS longitude,
  -- connection info
  CAST(master.connectivity AS VARCHAR) AS connectivity,
  CAST(master.connectivity_type_govt AS VARCHAR) AS connectivity_type_govt,
  CAST(master.cellular_coverage_type AS VARCHAR) AS cellular_coverage_type,
  CAST(master.school_area_type AS VARCHAR) AS school_area_type,
  CAST(master.school_funding_type AS VARCHAR) AS school_funding_type,
  CAST(master.education_level AS VARCHAR) AS education_level,
  CAST(master.electricity_availability AS VARCHAR) AS electricity_availability,
  CAST(master.fiber_node_distance AS DOUBLE) AS fiber_node_distance,
  -- other columns
  CAST(c.canton_name AS VARCHAR) AS canton_bih_only,
  CAST(cc.status AS VARCHAR) AS connectivity_credits_school,
  CAST(p.zaf_province AS VARCHAR) AS province_zaf_only,
  -- calculated columns
  -- to be removed once integrated with consistency indicator + IQB-edu
  CAST(CASE
    when EXTRACT(DAY FROM CURRENT_DATE - lm.last_measurement_date) <= 21 THEN 'live'
    when EXTRACT(DAY FROM CURRENT_DATE - lm.last_measurement_date) between 21 and 30 then 'at-risk'
    when EXTRACT(DAY FROM CURRENT_DATE - lm.last_measurement_date) >= 30 THEN 'drop-off'
    ELSE 'Unknown'
    END AS VARCHAR) AS install_status
from
  default.all_school_master   master

LEFT JOIN
  measurement_data data
ON
  master.school_id_giga = data.school_id_giga
LEFT JOIN
  max_app_version_per_school app
ON
  master.school_id_giga = app.school_id_giga
AND
  app.rt_source = 'GigaMeter'
LEFT JOIN
  last_measurement_date lm
ON
  master.school_id_giga = lm.school_id_giga
LEFT JOIN
  registration_attempts r
ON
  master.school_id_giga = r.school_id_giga
-- canton mapping
left join
  lstringer.bih_school_canton_mapping_vw c
on
  master.school_id_giga = c.school_id_giga
-- connectivty credits info
LEFT JOIN
  lstringer.connectivity_credit_pilot_schools cc
on
  master.school_id_giga = cc.school_id_giga

LEFT JOIN
  lstringer.zaf_province_mapping p
on
  master.school_id_giga = p.school_id_giga

  );
