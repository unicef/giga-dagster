

-- ==============================================================================
-- Script Name:     mng_gigameter_qos_measurements.sql
-- Table Created:   default.mng_gigameter_qos_measurements
-- Schema:          default
-- Region:          Mongolia
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Daily aggregated Mongolia school measurements combining two data sources:
--   1. LibreRouter (libre) — hardware-based monitoring devices installed in schools
--   2. GigaMeter app — mobile speed test application
--   Produces a unified daily view with source-specific metrics and a 'source'
--   field indicating which systems reported data for each school on each day.
--
-- Dependencies:
--   - qos.mng (aggregated LibreRouter measurements for Mongolia)
--   - default.all_gigameter_measurement_data_tb_physical (GigaMeter daily data)
--   - school_master.mng (Mongolia school master)
--
-- Output Columns:  ~25 columns
-- Primary Key:     school_id_giga + date
-- Granularity:     One row per school per day
--
-- Run Notes:
--   Recurring — refresh when either LibreRouter or GigaMeter data is updated.
--   'source' field: 'both', 'gigameter', or 'libre'. The commented-out
--   alternative CTE uses qos_raw.mng for finer-grained libre data if needed.
--
-- Last Updated:    2025-10-31 / Luke Stringer
-- ==============================================================================


CREATE TABLE IF NOT EXISTS default.mng_gigameter_qos_measurements AS  (



with

  master AS (
    SELECT DISTINCT school_id_govt,
      school_id_giga,
      school_name,
      admin1,
      admin2,
      connectivity,
      connectivity_type_govt,
      cellular_coverage_type,
      --school_area_type,
      --school_funding_type,
      education_level,
      electricity_availability,
      fiber_node_distance
    FROM school_master.mng
)

/*
, libre as (
    SELECT
      country_id,
      school_id_giga,
      school_id_govt,
      cast(date as date) as date,
      count(*) as num_measurements,
      min(speed_download) as speed_download_min,
      max(speed_download) as speed_download_max,
      avg(speed_download) as speed_download_mean,
      STDDEV(CAST(speed_download AS real)) AS speed_download_stdev,
      max (speed_upload) as speed_upload_max,
      min (speed_upload) as speed_upload_min,
      avg (speed_upload) as speed_upload_mean,
      STDDEV(CAST(speed_upload AS real)) AS speed_upload_stdev,
      sum(inbound_traffic)/ 1024 as inbound_traffic_sum,
      sum(outbound_traffic)/ 1024 as outbound_traffic_sum,
      --signature,
      --gigasync_id,
      'libre' as provider,
      1 AS libre_source
      -- select *
    FROM qos_raw.mng
    group by
      country_id,
      cast(date as date),
      school_id_giga,
      school_id_govt
  )
*/

, libre as (
SELECT
      country_id,
      school_id_giga,
      school_id_govt,
      cast(date as date) as date,
      sum(count) as num_measurements,
      min(speed_download_min) as speed_download_min,
      avg(speed_download_mean) as speed_download_mean,
      --STDDEV(CAST(speed_download_mean AS real)) AS speed_download_mean_stdev,
      max(speed_download_max) as speed_download_max,
      min(speed_upload_min) as speed_upload_min,
      avg(speed_upload_mean) as speed_upload_mean,
      max(speed_upload_max) as speed_upload_max,
      --STDDEV(CAST(speed_upload_mean AS real)) AS speed_upload_mean_stdev,
      sum(inbound_traffic_sum) / 1024 as inbound_traffic_sum,
      sum(outbound_traffic_sum) / 1024 as outbound_traffic_sum,
      --signature,
      --gigasync_id,
      provider,
      1 AS libre_source
      -- select *
    FROM qos.mng
    group by
      country_id,
      school_id_giga,
      school_id_govt,
      cast(date as date),
      provider
)

, gigameter as (

SELECT
  date,
  school_id_giga,
  sum(num_measurements) as num_measurements,
  count(DISTINCT device_id) as num_devices,
  avg(avg_download_speed) as avg_download_speed,
  avg(avg_upload_speed) as avg_upload_speed,
  avg(avg_latency) as avg_latency,
  max(detected_server) as detected_server,
  max(isp_clean) as isp_clean,
  max(isp_asn_clean) as isp_asn_clean,
  max(app_version) as app_version,
  1 as gigameter_source,
  'GigaMeter' as provider
from
  default.all_gigameter_measurement_data_tb_physical
where
  country = 'Mongolia'
group by
  date,
  school_id_giga

)


, measure as (
SELECT
  DISTINCT
    COALESCE(gigameter.school_id_giga, libre.school_id_giga) AS school_id_giga,
   COALESCE(gigameter.date, libre.date) AS date,
   (CASE WHEN libre.libre_source = 1 AND gigameter.gigameter_source = 1 THEN 'both'
      WHEN gigameter.gigameter_source = 1 AND libre.libre_source IS NULL THEN 'gigameter'
      WHEN libre.libre_source = 1 AND gigameter.gigameter_source IS NULL THEN 'libre'
      ELSE 'none' END) AS source,
    gigameter.avg_download_speed AS avg_download_speed_gigameter,
    gigameter.avg_upload_speed AS avg_upload_speed_gigameter,
    gigameter.avg_latency  AS avg_latency_gigameter,
    gigameter.isp_clean as detected_isp,
    gigameter.detected_server,
    gigameter.app_version as app_version_gigameter,
    gigameter.num_measurements as num_measurements_gigameter,
    gigameter.num_devices as num_devices_gigameter,
    libre.speed_download_mean AS avg_download_speed_libre,
    libre.speed_download_max AS max_download_speed_libre,
    --libre.speed_download_mean_stdev AS avg_download_speed_libre_stdev,
    libre.speed_upload_mean AS avg_upload_speed_libre,
    libre.speed_upload_max AS max_upload_speed_libre,
    --libre.speed_upload_mean_stdev AS avg_upload_speed_libre_stdev,
    libre.inbound_traffic_sum AS sum_inbound_traffic_libre,
    libre.outbound_traffic_sum AS sum_outbound_traffice_libre,
    libre.num_measurements as num_measurements_libre
FROM
  libre
FULL OUTER JOIN
  gigameter
ON
  gigameter.school_id_giga = libre.school_id_giga
AND
  gigameter.date = libre.date
ORDER BY date DESC

)


SELECT
  DISTINCT
    CAST(measure.school_id_giga AS VARCHAR) AS school_id_giga,
    CAST(measure.date AS DATE) AS date,
    CAST(measure.source AS VARCHAR) AS source,
    CAST(measure.avg_download_speed_gigameter AS DOUBLE) AS avg_download_speed_gigameter,
    CAST(measure.avg_upload_speed_gigameter AS DOUBLE) AS avg_upload_speed_gigameter,
    CAST(measure.avg_latency_gigameter AS DOUBLE) AS avg_latency_gigameter,
    CAST(measure.detected_isp AS VARCHAR) AS detected_isp,
    CAST(measure.detected_server AS VARCHAR) AS detected_server,
    CAST(measure.app_version_gigameter AS VARCHAR) AS app_version_gigameter,
    CAST(measure.num_measurements_gigameter AS BIGINT) AS num_measurements_gigameter,
    CAST(measure.num_devices_gigameter AS BIGINT) AS num_devices_gigameter,
    CAST(measure.avg_download_speed_libre AS DOUBLE) AS avg_download_speed_libre,
    CAST(measure.max_download_speed_libre AS DOUBLE) AS max_download_speed_libre,
    CAST(measure.avg_upload_speed_libre AS DOUBLE) AS avg_upload_speed_libre,
    CAST(measure.max_upload_speed_libre AS DOUBLE) AS max_upload_speed_libre,
    CAST(measure.sum_inbound_traffic_libre AS DOUBLE) AS sum_inbound_traffic_libre,
    CAST(measure.sum_outbound_traffice_libre AS DOUBLE) AS sum_outbound_traffice_libre,
    CAST(measure.num_measurements_libre AS BIGINT) AS num_measurements_libre,
    CAST(master.school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(master.school_name AS VARCHAR) AS school_name,
    CAST(master.admin1 AS VARCHAR) AS admin1,
    CAST(master.admin2 AS VARCHAR) AS admin2,
    CAST(master.connectivity AS VARCHAR) AS connectivity,
    CAST(master.connectivity_type_govt AS VARCHAR) AS connectivity_type_govt,
    CAST(master.cellular_coverage_type AS VARCHAR) AS cellular_coverage_type,
    CAST(master.education_level AS VARCHAR) AS education_level,
    CAST(master.electricity_availability AS VARCHAR) AS electricity_availability,
    CAST(master.fiber_node_distance AS DOUBLE) AS fiber_node_distance
FROM
  measure
LEFT JOIN
  master
ON
  master.school_id_giga = measure.school_id_giga

)
