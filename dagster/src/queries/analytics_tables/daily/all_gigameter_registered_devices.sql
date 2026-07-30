






-- ==============================================================================
-- Script Name:     all_gigameter_registered_devices.sql
-- Table Created:   test_tables.all_gigameter_registered_devices
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Device-level summary of GigaMeter registration and measurement history.
--   Each row represents one registered device and captures its activity,
--   associated school, connectivity status, and the most frequently observed
--   ISP and server. Complements all_gigameter_registered_schools.sql with
--   finer-grained device-level visibility.
--
-- Dependencies:
--   - gigameter_production_db.public.device (device registration records)
--   - test_tables.all_gigameter_measurement_data (consolidated measurement history)
--   - test_tables.all_school_master (school metadata and geography)
--   - test_tables.all_gigameter_appversion_funnel (app version history per device)
--
-- Output Columns:  ~30 columns
-- Primary Key:     device_id + school_id_giga
-- Granularity:     One row per registered device-school combination
--
-- Run Notes:
--   Recurring — refresh daily or on-demand. Connectivity status thresholds
--   mirror all_gigameter_registered_schools.sql (live / at-risk / drop-off).
--
-- Last Updated:    2025-10-31 / Luke Stringer
-- ==============================================================================

 CREATE TABLE IF NOT EXISTS test_tables.all_gigameter_registered_devices as (

with

devices as (

SELECT
  DISTINCT
    s.device_hardware_id as device_id,
    s.user_id,
    s.installed_path,
    s.is_active,
    s.windows_username,
    s.wifi_connections,
    TRY(CAST(parse_datetime(s.created, 'yyyy-MM-ddah:mm:ssXXX') AS TIMESTAMP)) as device_registration_date,
    s.app_version as initial_device_app_version,
    'GigaMeter' as rt_source,
    m.unicef_region,
    m.country,
    m.iso3_code,
    m.latitude,
    m.longitude,
    s.giga_id_school as school_id_giga,
    m.school_id_govt,
    m.school_name
  -- select *
FROM
  gigameter_production_db.public.dailycheckapp_school s
left join
  test_tables.all_school_master m
on
  m.school_id_giga = s.giga_id_school




)


, last_measurement_info as (


SELECT
  DISTINCT
    school_id_govt,
    school_id_giga,
    device_id,
    browser_id,
    last_app_version,
    last_measurement_date,
    rt_source
FROM
    (
        SELECT
            school_id_govt,
            school_id_giga,
            device_id,
            browser_id,
            app_version AS last_app_version,
            local_created_timestamp AS last_measurement_date,
            ROW_NUMBER() OVER (PARTITION BY rt_source, device_id, browser_id,  school_id_giga ORDER BY created_timestamp DESC) AS row_num,
            rt_source
            -- select *
        FROM
            test_tables.all_gigameter_measurement_data
    ) AS a
WHERE
    row_num = 1



)



, measurement_data as (


SELECT
  DISTINCT
    m.browser_id,
    m.device_id,
    m.school_id_giga,
    m.school_id_govt,
    m.school_name,
    m.country,
    m.iso3_code,
    approx_most_frequent(5, m.detected_server, 5) as detected_server_mode,
    approx_most_frequent(5, m.isp_name, 5) as detected_isp,
    approx_most_frequent(5, m.isp_asn, 5) as detected_isp_asn,
    min(m.local_created_timestamp) as first_measurement_date,
    count(distinct m.local_created_timestamp) as num_measurements,
    count(distinct date_trunc('day', m.local_created_timestamp)) AS num_days_measured
    -- select *
FROM
  test_tables.all_gigameter_measurement_data m


group by
    browser_id,
    device_id,
    school_id_giga,
    school_id_govt,
    school_name,
    country,
    iso3_code
)




 , pre_table as (

select
  DISTINCT
  COALESCE(measure.iso3_code, devices.iso3_code) as iso3_code,
  COALESCE(measure.country, devices.country) as country,
  COALESCE(devices.school_id_giga, measure.school_id_giga) as school_id_giga,
  COALESCE(measure.school_id_govt, devices.school_id_govt) as school_id_govt,
  COALESCE(devices.school_name, measure.school_name) as school_name,
  COALESCE(devices.device_id, measure.device_id) as device_id,
  COALESCE(devices.user_id, measure.browser_id) as user_browser_id,
  devices.device_registration_date as initial_registration_date,
  ap.most_recent_app_version_clean as most_recent_app_version,
  devices.initial_device_app_version as initial_app_version,
  devices.installed_path,
  devices.is_active,
  devices.windows_username,
  devices.wifi_connections,
  measure.first_measurement_date,
  EXTRACT(DAY FROM CURRENT_DATE - lm.last_measurement_date) AS days_since_last_measurement,
  measure.num_measurements,
  measure.num_days_measured,
  lm.last_measurement_date,
  lm.last_app_version as latest_measurement_app_version,
  lm.rt_source as last_measurement_source,
  measure.detected_server_mode,
  measure.detected_isp as detected_isp_mode,
  measure.detected_isp_asn as detected_isp_asn_mode
  --element_at(map_keys(measure.detected_server_mode),1) as primary_server,
  --element_at(map_keys(measure.detected_server_mode),2) as secondary_server,
  --element_at(map_keys(measure.detected_isp),1) as primary_isp,
  --element_at(map_keys(measure.detected_isp_asn),1) as primary_isp_asn,
  --element_at(map_keys(measure.detected_isp),2) as secondary_isp,
  --element_at(map_keys(measure.detected_isp_asn),2) as secondary_isp_asn
FROM
  devices
inner JOIN
  measurement_data measure
on
--  devices.device_id = measure.device_id
--and
  devices.user_id = measure.browser_id
and
  devices.school_id_giga = measure.school_id_giga
LEFT JOIN
  test_tables.all_gigameter_appversion_funnel ap
ON
  measure.school_id_giga = ap.school_id_giga
--AND
--  measure.device_id = ap.device_id
AND
  measure.browser_id = ap.browser_id
LEFT JOIN
  last_measurement_info lm
ON
  measure.school_id_giga = lm.school_id_giga
--and
--  measure.device_id = lm.device_id
and
  measure.browser_id = lm.browser_id

)



SELECT
  DISTINCT
  CAST(pt.iso3_code AS VARCHAR) AS iso3_code,
  CAST(pt.country AS VARCHAR) AS country,
  CAST(pt.school_id_giga AS VARCHAR) AS school_id_giga,
  CAST(pt.school_id_govt AS VARCHAR) AS school_id_govt,
  CAST(pt.device_id AS VARCHAR) AS device_id,
  CAST(pt.user_browser_id AS VARCHAR) AS user_browser_id,
  CAST(pt.school_name AS VARCHAR) AS school_name,
  CAST(pt.initial_registration_date AS TIMESTAMP) AS initial_registration_date,
  CAST(pt.initial_app_version AS VARCHAR) AS initial_app_version,
  CAST(pt.installed_path AS VARCHAR) AS installed_path,
  CAST(pt.is_active AS BOOLEAN) AS is_active,
  CAST(pt.windows_username AS VARCHAR) AS windows_username,
  CAST(json_format(cast(pt.wifi_connections as json)) AS VARCHAR) as wifi_connections,
  CAST(pt.most_recent_app_version AS VARCHAR) AS most_recent_app_version,
  CAST(pt.first_measurement_date AS TIMESTAMP) AS first_measurement_date,
  CAST(pt.last_measurement_date AS TIMESTAMP) AS last_measurement_date,
  CAST(pt.latest_measurement_app_version AS VARCHAR) AS latest_measurement_app_version,
  CAST(pt.last_measurement_source AS VARCHAR) AS last_measurement_source,
  CAST(pt.days_since_last_measurement AS DOUBLE) AS days_since_last_measurement,
  CAST(pt.num_measurements AS BIGINT) AS num_measurements,
  CAST(pt.num_days_measured AS BIGINT) AS num_days_measured,
  pt.detected_server_mode,
  pt.detected_isp_asn_mode,
  pt.detected_isp_mode,
 -- pt.primary_isp,
 -- pt.primary_isp_asn,
 -- pt.secondary_isp,
 -- pt.secondary_isp_asn,
-- custom fields
  CAST(CASE WHEN pt.initial_registration_date IS NULL THEN 'No' ELSE 'Yes' END AS VARCHAR) AS registered_gigameter,
  CAST(CASE WHEN pt.initial_app_version IS NULL THEN 'No' ELSE 'Yes' END AS VARCHAR) AS installed_gigameter,
  CAST(CASE WHEN pt.first_measurement_date IS NULL THEN 'No' ELSE 'Yes' END AS VARCHAR) AS sending_gigameter_data,
  CAST(CASE
    WHEN pt.days_since_last_measurement <= 21 THEN 'live'
    WHEN pt.days_since_last_measurement BETWEEN 21 AND 30 THEN 'at-risk'
    WHEN pt.days_since_last_measurement >= 30 THEN 'drop-off'
    ELSE 'Unknown'
  END AS VARCHAR) AS install_status,
  CAST(c.canton_name AS VARCHAR) AS canton_bih_only,
  CAST(CASE WHEN cc.status IS NULL THEN 'not included' ELSE cc.status END AS VARCHAR) AS connectivity_credits_school,
  CAST(p.zaf_province AS VARCHAR) AS province_zaf_only
FROM
  pre_table pt
left join
  lstringer.bih_school_canton_mapping_vw c
on
  pt.school_id_giga = c.school_id_giga
LEFT JOIN
  lstringer.connectivity_credit_pilot_schools cc
on
  pt.school_id_giga = cc.school_id_giga

LEFT JOIN lstringer.zaf_province_mapping p
  on pt.school_id_giga = p.school_id_giga

  )
