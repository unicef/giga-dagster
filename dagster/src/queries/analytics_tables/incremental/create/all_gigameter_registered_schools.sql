-- all_gigameter_registered_schools (incremental/create)
-- One row per (country, school_id_giga). Runs once when the table is missing;
-- later runs use ../update/. Keep the SELECT identical to update/'s MERGE source.

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


-- most recent install per school (device_hardware_id often NULL at source)
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


, registered AS (
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
  -- connectivity (master) and connectivity_gigamaps (GigaMaps) are different signals
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
    END AS VARCHAR) AS install_status,
  ROW_NUMBER() OVER (
    PARTITION BY master.country, master.school_id_giga
    -- dedupe: MERGE source must be unique per key
    ORDER BY master.school_id_govt, master.school_name, gmconn.connectivity_gigamaps,
             master.connectivity, master.connectivity_type_govt, master.admin1, master.admin2,
             master.latitude, master.longitude, master.education_level, master.school_area_type,
             c.canton_name, cc.status, p.zaf_province, l.zone
  ) AS rn
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


-- final query
SELECT
  country,
  iso3_code,
  school_id_giga,
  school_name,
  school_id_govt,
  registered_gigameter,
  sending_gigameter_data,
  first_measurement_date,
  last_measurement_date,
  days_since_last_measurement,
  measurements_per_source,
  num_devices_registered,
  devices_still_logged_in,
  num_devices_measured,
  max_app_version_gigameter,
  last_device_installed_id,
  last_device_installed_date,
  detected_isp_nested,
  detected_isp_asn_nested,
  detected_server_nested,
  admin1,
  admin2,
  unicef_region,
  latitude,
  longitude,
  connectivity,
  connectivity_gigamaps,
  connectivity_type_govt,
  cellular_coverage_type,
  school_area_type,
  school_funding_type,
  education_level,
  electricity_availability,
  fiber_node_distance,
  canton_bih_only,
  connectivity_credits_school,
  province_zaf_only,
  education_zone_lka_only,
  install_status,
  CAST(FALSE AS BOOLEAN) AS is_deleted,
  CAST(NULL AS TIMESTAMP) AS deleted_at
FROM
  registered
WHERE
  rn = 1

)
