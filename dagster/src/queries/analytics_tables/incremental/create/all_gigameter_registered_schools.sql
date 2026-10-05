-- ==============================================================================
-- Script Name:     all_gigameter_registered_schools.sql (incremental/create)
-- Table Created:   default.all_gigameter_registered_schools
--
-- !! KEEP IN SYNC: the SELECT below must stay IDENTICAL to the one in
-- !! ../update/all_gigameter_registered_schools.sql -- the two files differ
-- !! only in CREATE TABLE (create/) vs CREATE OR REPLACE TABLE (update/).
--
-- Purpose:
--   School-level summary of GigaMeter registration and measurement activity.
--   Each row represents one school in all_school_master and captures
--   registration status, measurement sending status, connectivity status
--   (live / at-risk / drop-off), and the most frequently observed ISP / ASN /
--   server / source.
--
-- Pattern:         Full rebuild every HOUR, but cheap: lifetime measurement
--   aggregates come pre-computed from the incremental accumulator
--   default.all_gigameter_school_measurement_stats instead of scanning
--   all_gigameter_measurement_data. Whole-table rebuild rather than a row-level
--   MERGE because master attributes, registrations, lookups and the
--   CURRENT_DATE-relative columns change for reasons that can't be detected
--   without building the full join anyway. create/ runs once (table missing);
--   update/ swaps in the new version atomically with CREATE OR REPLACE TABLE
--   (no window where the table is missing, Delta history kept).
--
-- Dependencies:
--   - default.all_school_master (school metadata and geography; one row each)
--   - default.all_gigameter_school_measurement_stats (incremental accumulator)
--   - gigameter_production_db.public.dailycheckapp_school (registration/check-in events)
--   - lstringer.school_connectivity_status_vw (GigaMaps connectivity classification
--     -- see analytics-tables/views/school_connectivity_status_vw.sql)
--   - lstringer.bih_school_canton_mapping_vw, lstringer.connectivity_credit_pilot_schools,
--     lstringer.zaf_province_mapping, lstringer.lka_school_education_zones (lookups)
--
-- Output Columns:  39 (unchanged names, order and types)
-- Primary Key:     school_id_giga
-- Granularity:     One row per school in all_school_master
--
-- Run Notes:
--   Connectivity status thresholds: live <=21 days since last measurement,
--   at-risk 21-30 days, drop-off >30 days.
--   first_measurement_date AND last_measurement_date are both school-local time
--   (local_created_timestamp). Until 2026-10 last_measurement_date was UTC
--   (created_timestamp); days_since_last_measurement and install_status follow it.
--   measurements_per_source / detected_isp_nested / detected_isp_asn_nested /
--   detected_server_nested are the EXACT top 10 values by measurement count
--   (ties broken by key ascending), keys ordered most-frequent first -- replaced
--   approx_most_frequent(5, x, 5) in 2026-10. mng_gigameter_qos_registered reads
--   element_at(map_keys(...), 1/2) and relies on that order.
--   last_device_installed_id is device_hardware_id from the most recent
--   dailycheckapp_school row per school -- only ~31% populated at source,
--   NULL is expected/common, not a bug. last_device_installed_date is
--   separately ~75% populated.
--
-- Partitioned by: country (12 Superset datasets on dashboard 21 filter this
-- table by a single country each -- unpartitioned, every one of them scanned
-- the full table)
--
-- Last Updated:    2026-10-05 / Luke Stringer
-- ==============================================================================

CREATE TABLE delta_lake.default.all_gigameter_registered_schools
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_registered_schools',
    partitioned_by = ARRAY['country']
)
AS (

WITH

-- num times registered
registration_attempts AS (
    SELECT
        giga_id_school AS school_id_giga,
        COUNT(DISTINCT user_id) AS num_devices_registered,
        MIN(created_at) AS first_registration_attempt,
        SUM(CASE WHEN is_active = FALSE THEN 0 ELSE 1 END) AS devices_still_logged_in        -- inverse count of the number of devices logged out to get devices still logged in
    FROM
        gigameter_production_db.public.dailycheckapp_school
    GROUP BY
        giga_id_school
)


-- most recently installed device per school (by registration/check-in event, not by
-- currently-active status -- an install stays "most recent" even if later logged out).
-- device_hardware_id is only ~31% populated at source; NULL here is expected.
, last_device_installed AS (
    SELECT
        giga_id_school AS school_id_giga,
        device_hardware_id AS last_device_installed_id,
        created_at AS last_device_installed_date
    FROM (
        SELECT
            giga_id_school,
            device_hardware_id,
            created_at,
            ROW_NUMBER() OVER (PARTITION BY giga_id_school ORDER BY created_at DESC) AS row_num
        FROM
            gigameter_production_db.public.dailycheckapp_school
    ) AS d
    WHERE row_num = 1
)


-- final query
SELECT
  CAST(master.country AS VARCHAR) AS country,
  CAST(master.iso3_code AS VARCHAR) AS iso3_code,
  CAST(master.school_id_giga AS VARCHAR) AS school_id_giga,
  CAST(master.school_name AS VARCHAR) AS school_name,
  CAST(master.school_id_govt AS VARCHAR) AS school_id_govt,
  -- registration info
  CAST(CASE WHEN r.first_registration_attempt IS NOT NULL THEN 'Yes' ELSE 'No' END AS VARCHAR) AS registered_gigameter,
  CAST(CASE WHEN st.first_measurement_date IS NOT NULL THEN 'Yes' ELSE 'No' END AS VARCHAR) AS sending_gigameter_data,
  CAST(st.first_measurement_date AS TIMESTAMP) AS first_measurement_date,
  CAST(st.last_measurement_date AS TIMESTAMP) AS last_measurement_date,
  CAST(EXTRACT(DAY FROM CURRENT_DATE - st.last_measurement_date) AS DOUBLE) AS days_since_last_measurement,
  -- rt sources: exact top 10 by count, most frequent first
  map_from_entries(slice(array_sort(map_entries(st.source_counts),
      (x, y) -> CASE WHEN x[2] > y[2] THEN -1 WHEN x[2] < y[2] THEN 1
                     WHEN x[1] < y[1] THEN -1 WHEN x[1] > y[1] THEN 1 ELSE 0 END), 1, 10)) AS measurements_per_source,
  -- devices
  CAST(r.num_devices_registered AS BIGINT) AS num_devices_registered,
  CAST(r.devices_still_logged_in AS BIGINT) AS devices_still_logged_in,
  CAST(CASE WHEN st.school_id_giga IS NOT NULL THEN COALESCE(cardinality(st.device_ids), 0) END AS BIGINT) AS num_devices_measured,
  CAST(st.max_app_version_gigameter AS VARCHAR) AS max_app_version_gigameter,
  CAST(dev.last_device_installed_id AS VARCHAR) AS last_device_installed_id,
  CAST(dev.last_device_installed_date AS TIMESTAMP) AS last_device_installed_date,
  -- provider info: exact top 10 by count, most frequent first
  map_from_entries(slice(array_sort(map_entries(st.isp_counts),
      (x, y) -> CASE WHEN x[2] > y[2] THEN -1 WHEN x[2] < y[2] THEN 1
                     WHEN x[1] < y[1] THEN -1 WHEN x[1] > y[1] THEN 1 ELSE 0 END), 1, 10)) AS detected_isp_nested,
  map_from_entries(slice(array_sort(map_entries(st.isp_asn_counts),
      (x, y) -> CASE WHEN x[2] > y[2] THEN -1 WHEN x[2] < y[2] THEN 1
                     WHEN x[1] < y[1] THEN -1 WHEN x[1] > y[1] THEN 1 ELSE 0 END), 1, 10)) AS detected_isp_asn_nested,
  map_from_entries(slice(array_sort(map_entries(st.server_counts),
      (x, y) -> CASE WHEN x[2] > y[2] THEN -1 WHEN x[2] < y[2] THEN 1
                     WHEN x[1] < y[1] THEN -1 WHEN x[1] > y[1] THEN 1 ELSE 0 END), 1, 10)) AS detected_server_nested,
  -- additional geography
  CAST(master.admin1 AS VARCHAR) AS admin1,
  CAST(master.admin2 AS VARCHAR) AS admin2,
  CAST(master.unicef_region AS VARCHAR) AS unicef_region,
  CAST(master.latitude AS DOUBLE) AS latitude,
  CAST(master.longitude AS DOUBLE) AS longitude,
  -- connection info
  -- connectivity: from all_school_master (government-reported, optionally
  -- overridden by real-time GigaMeter/MLab/QoS measurement presence) --
  -- unchanged name/values, kept as-is for downstream dashboard compatibility.
  -- connectivity_gigamaps: from GigaMaps' own weekly connectivity monitoring, a
  -- separate signal -- see analytics-tables/views/school_connectivity_status_vw.sql.
  -- These two can and do disagree (investigated 2026-07-31) -- do not assume they
  -- mean the same thing.
  CAST(master.connectivity AS VARCHAR) AS connectivity,
  CAST(gmconn.connectivity_gigamaps AS VARCHAR) AS connectivity_gigamaps,
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
  CAST(l.zone AS VARCHAR) AS education_zone_lka_only,
  -- calculated columns
  -- to be removed once integrated with consistency indicator + IQB-edu
  CAST(CASE
    WHEN EXTRACT(DAY FROM CURRENT_DATE - st.last_measurement_date) <= 21 THEN 'live'
    WHEN EXTRACT(DAY FROM CURRENT_DATE - st.last_measurement_date) BETWEEN 21 AND 30 THEN 'at-risk'
    WHEN EXTRACT(DAY FROM CURRENT_DATE - st.last_measurement_date) >= 30 THEN 'drop-off'
    ELSE 'Unknown'
    END AS VARCHAR) AS install_status
FROM
  delta_lake.default.all_school_master master

LEFT JOIN
  delta_lake.default.all_gigameter_school_measurement_stats st
ON
  master.school_id_giga = st.school_id_giga
LEFT JOIN
  registration_attempts r
ON
  master.school_id_giga = r.school_id_giga
LEFT JOIN
  last_device_installed dev
ON
  master.school_id_giga = dev.school_id_giga
LEFT JOIN
  lstringer.school_connectivity_status_vw gmconn
ON
  master.school_id_giga = gmconn.school_id_giga
-- canton mapping
LEFT JOIN
  lstringer.bih_school_canton_mapping_vw c
ON
  master.school_id_giga = c.school_id_giga
-- connectivty credits info
LEFT JOIN
  lstringer.connectivity_credit_pilot_schools cc
ON
  master.school_id_giga = cc.school_id_giga

LEFT JOIN
  lstringer.zaf_province_mapping p
ON
  master.school_id_giga = p.school_id_giga

LEFT JOIN lstringer.lka_school_education_zones l
  ON master.school_id_govt = l.school_id_govt
  AND master.iso3_code = l.iso3_code

)
