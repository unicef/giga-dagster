-- ==============================================================================
-- Script Name:     all_gigameter_registered_schools.sql (incremental/update)
-- Table Updated:   default.all_gigameter_registered_schools
--
-- !! KEEP IN SYNC: the SELECT below must stay IDENTICAL to the MERGE source in
-- !! ../create/all_gigameter_registered_schools.sql -- create/ wraps it in CREATE TABLE AS,
-- !! update/ uses it as the USING (...) source of the first MERGE.
--
-- Purpose:
--   School-level summary of GigaMeter registration and measurement activity.
--   Each row represents one school in all_school_master and captures
--   registration status, measurement sending status, connectivity status
--   (live / at-risk / drop-off), and the most frequently observed ISP / ASN /
--   server / source.
--
-- Pattern:         Hourly MERGE + deleted flag (no rows are ever removed).
--   1. ALTER TABLE ... ADD COLUMN IF NOT EXISTS is_deleted / deleted_at
--      (no-op once present; lets a table built before 2026-10 take the MERGE).
--   2. MERGE the full current SELECT (identical to ../create/) on
--      (school_id_giga, country):
--        WHEN MATCHED and any column differs -> UPDATE all columns
--          (also un-flags a school that has come back to the master);
--        WHEN NOT MATCHED                    -> INSERT.
--      The IS DISTINCT FROM guard means unchanged rows are not rewritten.
--      Master attributes, registrations, lookups and the CURRENT_DATE-relative
--      columns change for reasons only the full join can detect, so the
--      SELECT is always rebuilt in full -- cheap, because the lifetime
--      measurement aggregates come from the incremental accumulator
--      default.all_gigameter_school_measurement_stats.
--   3. MERGE flag: rows whose (school_id_giga, country) is no longer in
--      all_school_master get is_deleted = TRUE, deleted_at = run time.
--      Trino MERGE has no WHEN NOT MATCHED BY SOURCE, hence the second MERGE;
--      its source is DISTINCT so duplicate target rows cannot fail it.
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
-- Output Columns:  41 -- the original 39 (unchanged names, order and types)
--                  + is_deleted BOOLEAN, deleted_at TIMESTAMP (added 2026-10)
-- Primary Key:     (country, school_id_giga) -- deduplicated, see Run Notes
-- Granularity:     One row per school ever seen in all_school_master; schools
--                  that have left the master stay, flagged is_deleted = TRUE
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
--   Dedupe: all_school_master holds ~58 duplicate school_id_giga and the
--   lookups / school_connectivity_status_vw can repeat a school, which used to
--   emit ~174 duplicate rows. The SELECT keeps exactly one row per
--   (country, school_id_giga), chosen deterministically (ROW_NUMBER ORDER BY
--   master/lookup columns) -- required for the MERGE source, and keeps
--   create/ and update/ output identical.
--   Departed schools: downstream counts of "schools" should filter
--   COALESCE(is_deleted, FALSE) = FALSE.
--
-- Partitioned by: country (12 Superset datasets on dashboard 21 filter this
-- table by a single country each -- unpartitioned, every one of them scanned
-- the full table)
--
-- Last Updated:    2026-10-19 / Luke Stringer
-- ==============================================================================

-- 1. new columns (no-op once present)
ALTER TABLE delta_lake.default.all_gigameter_registered_schools ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN;

ALTER TABLE delta_lake.default.all_gigameter_registered_schools ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;


-- 2. insert new schools, update changed ones
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


    -- one row per school, before dedupe
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
        END AS VARCHAR) AS install_status,
      ROW_NUMBER() OVER (
        PARTITION BY master.country, master.school_id_giga
        -- deterministic pick when all_school_master or a lookup holds duplicate
        -- rows for the same school (a MERGE source must be unique per key)
        ORDER BY master.school_id_govt, master.school_name, gmconn.connectivity_gigamaps,
                 master.connectivity, master.connectivity_type_govt, master.admin1, master.admin2,
                 master.latitude, master.longitude, master.education_level, master.school_area_type,
                 c.canton_name, cc.status, p.zaf_province, l.zone   -- Trino default: ASC NULLS LAST
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


    -- final query: one row per (country, school_id_giga)
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


-- 3. flag schools that have left all_school_master (rows are kept, never deleted)
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
