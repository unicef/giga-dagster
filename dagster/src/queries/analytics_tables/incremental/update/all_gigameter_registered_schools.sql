-- all_gigameter_registered_schools (incremental/update)
-- Hourly: MERGE changed/new schools, then flag schools no longer in
-- all_school_master as is_deleted (rows are never removed).
-- Keep the SELECT identical to ../create/.

ALTER TABLE delta_lake.default.all_gigameter_registered_schools ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN;

ALTER TABLE delta_lake.default.all_gigameter_registered_schools ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;


MERGE INTO delta_lake.default.all_gigameter_registered_schools AS t
USING (

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

) AS s
ON t.school_id_giga = s.school_id_giga
AND t.country = s.country

WHEN MATCHED AND (
    t.country IS DISTINCT FROM s.country
    OR t.iso3_code IS DISTINCT FROM s.iso3_code
    OR t.school_id_giga IS DISTINCT FROM s.school_id_giga
    OR t.school_name IS DISTINCT FROM s.school_name
    OR t.school_id_govt IS DISTINCT FROM s.school_id_govt
    OR t.registered_gigameter IS DISTINCT FROM s.registered_gigameter
    OR t.sending_gigameter_data IS DISTINCT FROM s.sending_gigameter_data
    OR t.first_measurement_date IS DISTINCT FROM s.first_measurement_date
    OR t.last_measurement_date IS DISTINCT FROM s.last_measurement_date
    OR t.days_since_last_measurement IS DISTINCT FROM s.days_since_last_measurement
    OR t.measurements_per_source IS DISTINCT FROM s.measurements_per_source
    OR t.num_devices_registered IS DISTINCT FROM s.num_devices_registered
    OR t.devices_still_logged_in IS DISTINCT FROM s.devices_still_logged_in
    OR t.num_devices_measured IS DISTINCT FROM s.num_devices_measured
    OR t.max_app_version_gigameter IS DISTINCT FROM s.max_app_version_gigameter
    OR t.last_device_installed_id IS DISTINCT FROM s.last_device_installed_id
    OR t.last_device_installed_date IS DISTINCT FROM s.last_device_installed_date
    OR t.detected_isp_nested IS DISTINCT FROM s.detected_isp_nested
    OR t.detected_isp_asn_nested IS DISTINCT FROM s.detected_isp_asn_nested
    OR t.detected_server_nested IS DISTINCT FROM s.detected_server_nested
    OR t.admin1 IS DISTINCT FROM s.admin1
    OR t.admin2 IS DISTINCT FROM s.admin2
    OR t.unicef_region IS DISTINCT FROM s.unicef_region
    OR t.latitude IS DISTINCT FROM s.latitude
    OR t.longitude IS DISTINCT FROM s.longitude
    OR t.connectivity IS DISTINCT FROM s.connectivity
    OR t.connectivity_gigamaps IS DISTINCT FROM s.connectivity_gigamaps
    OR t.connectivity_type_govt IS DISTINCT FROM s.connectivity_type_govt
    OR t.cellular_coverage_type IS DISTINCT FROM s.cellular_coverage_type
    OR t.school_area_type IS DISTINCT FROM s.school_area_type
    OR t.school_funding_type IS DISTINCT FROM s.school_funding_type
    OR t.education_level IS DISTINCT FROM s.education_level
    OR t.electricity_availability IS DISTINCT FROM s.electricity_availability
    OR t.fiber_node_distance IS DISTINCT FROM s.fiber_node_distance
    OR t.canton_bih_only IS DISTINCT FROM s.canton_bih_only
    OR t.connectivity_credits_school IS DISTINCT FROM s.connectivity_credits_school
    OR t.province_zaf_only IS DISTINCT FROM s.province_zaf_only
    OR t.education_zone_lka_only IS DISTINCT FROM s.education_zone_lka_only
    OR t.install_status IS DISTINCT FROM s.install_status
    OR t.is_deleted IS DISTINCT FROM s.is_deleted
) THEN UPDATE SET
    country = s.country,
    iso3_code = s.iso3_code,
    school_id_giga = s.school_id_giga,
    school_name = s.school_name,
    school_id_govt = s.school_id_govt,
    registered_gigameter = s.registered_gigameter,
    sending_gigameter_data = s.sending_gigameter_data,
    first_measurement_date = s.first_measurement_date,
    last_measurement_date = s.last_measurement_date,
    days_since_last_measurement = s.days_since_last_measurement,
    measurements_per_source = s.measurements_per_source,
    num_devices_registered = s.num_devices_registered,
    devices_still_logged_in = s.devices_still_logged_in,
    num_devices_measured = s.num_devices_measured,
    max_app_version_gigameter = s.max_app_version_gigameter,
    last_device_installed_id = s.last_device_installed_id,
    last_device_installed_date = s.last_device_installed_date,
    detected_isp_nested = s.detected_isp_nested,
    detected_isp_asn_nested = s.detected_isp_asn_nested,
    detected_server_nested = s.detected_server_nested,
    admin1 = s.admin1,
    admin2 = s.admin2,
    unicef_region = s.unicef_region,
    latitude = s.latitude,
    longitude = s.longitude,
    connectivity = s.connectivity,
    connectivity_gigamaps = s.connectivity_gigamaps,
    connectivity_type_govt = s.connectivity_type_govt,
    cellular_coverage_type = s.cellular_coverage_type,
    school_area_type = s.school_area_type,
    school_funding_type = s.school_funding_type,
    education_level = s.education_level,
    electricity_availability = s.electricity_availability,
    fiber_node_distance = s.fiber_node_distance,
    canton_bih_only = s.canton_bih_only,
    connectivity_credits_school = s.connectivity_credits_school,
    province_zaf_only = s.province_zaf_only,
    education_zone_lka_only = s.education_zone_lka_only,
    install_status = s.install_status,
    is_deleted = s.is_deleted,
    deleted_at = s.deleted_at

WHEN NOT MATCHED THEN INSERT (
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
    is_deleted,
    deleted_at
) VALUES (
    s.country,
    s.iso3_code,
    s.school_id_giga,
    s.school_name,
    s.school_id_govt,
    s.registered_gigameter,
    s.sending_gigameter_data,
    s.first_measurement_date,
    s.last_measurement_date,
    s.days_since_last_measurement,
    s.measurements_per_source,
    s.num_devices_registered,
    s.devices_still_logged_in,
    s.num_devices_measured,
    s.max_app_version_gigameter,
    s.last_device_installed_id,
    s.last_device_installed_date,
    s.detected_isp_nested,
    s.detected_isp_asn_nested,
    s.detected_server_nested,
    s.admin1,
    s.admin2,
    s.unicef_region,
    s.latitude,
    s.longitude,
    s.connectivity,
    s.connectivity_gigamaps,
    s.connectivity_type_govt,
    s.cellular_coverage_type,
    s.school_area_type,
    s.school_funding_type,
    s.education_level,
    s.electricity_availability,
    s.fiber_node_distance,
    s.canton_bih_only,
    s.connectivity_credits_school,
    s.province_zaf_only,
    s.education_zone_lka_only,
    s.install_status,
    s.is_deleted,
    s.deleted_at
);


-- flag schools no longer in all_school_master
MERGE INTO delta_lake.default.all_gigameter_registered_schools AS t
USING (
    SELECT DISTINCT
        r.school_id_giga,
        r.country
    FROM delta_lake.default.all_gigameter_registered_schools r
    LEFT JOIN delta_lake.default.all_school_master m
        ON m.school_id_giga = r.school_id_giga
       AND m.country = r.country
    WHERE m.school_id_giga IS NULL
      AND COALESCE(r.is_deleted, FALSE) = FALSE
) AS s
ON t.school_id_giga = s.school_id_giga
AND t.country = s.country

WHEN MATCHED THEN UPDATE SET
    is_deleted = TRUE,
    deleted_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP);
