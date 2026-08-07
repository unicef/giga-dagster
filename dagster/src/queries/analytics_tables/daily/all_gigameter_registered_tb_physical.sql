




-- ==============================================================================
-- Script Name:     all_gigameter_registered_tb_physical.sql
-- Table Created:   default.all_gigameter_registered_tb_physical
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   School-level summary table for GigaMeter/MLAB registered schools. Provides
--   one row per school with registration status, measurement counts, device
--   information, and funnel tracking. Used for monitoring school onboarding,
--   engagement, and connectivity status across the GigaMeter program.
--
-- Dependencies:
--   - default.all_gigameter_measurement_data_tb_physical (measurement data)
--   - default.all_school_master (school metadata)
--   - default.all_gigameter_appversion_funnel (app version tracking)
--   - default.all_gigameter_schools_initial_registration_date_vw (registration dates)
--   - default.all_gigameter_schools_funnel_status_vw (funnel status)
--   - gigameter_production_db.public.dailycheckapp_school (registration attempts)
--   - gigamaps_production_db.public.connection_statistics_schoolweeklystatus (GigaMaps status)
--   - lstringer.bih_school_canton_mapping_vw (Bosnia canton mapping - optional)
--   - lstringer.connectivity_credit_pilot_schools (connectivity credits - optional)
--
-- Output Columns:  ~50 columns
-- Primary Key:     school_id_giga
-- Approximate Rows: One row per school with any GigaMeter/MLAB activity
--
-- CAVEATS:
--   - primary_source indicates main data source: 'GigaMeter' or 'Mlab'
--   - install_status based on days_since_last_measurement:
--     * 'live' = <= 21 days
--     * 'at-risk' = 21-30 days
--     * 'drop-off' = >= 30 days
--   - registered_gigameter/installed_gigameter/sending_gigameter_data are derived flags
--   - connectivity_gigamaps field is specific to Bosnia and Herzegovina
--   - approx_most_frequent() used for ISP/server detection (approximate mode)
--   - FULL JOIN with all_school_master means includes schools without measurements
--
-- Last Updated:    2025-10-14 / Luke Stringer
-- ==============================================================================


CREATE TABLE IF NOT EXISTS default.all_gigameter_registered_tb_physical
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_registered_tb_physical'
)
AS (

WITH


-- ==============================================================================
-- CTE: last_measurement_date
-- Purpose: Gets the most recent measurement per school per data source
-- Source:  default.all_gigameter_measurement_data_tb_physical
-- Method:  ROW_NUMBER() window function to find latest record
-- Output:  school_id_giga, last_app_version, last_measurement_date, rt_source
-- ==============================================================================
last_measurement_date AS (
    SELECT DISTINCT
        school_id_govt,
        school_id_giga,
        last_app_version,
        last_measurement_date,
        rt_source
    FROM (
        SELECT
            school_id_govt,
            school_id_giga,
            app_version AS last_app_version,
            local_created_timestamp AS last_measurement_date,
            -- Partition by source to get latest per source type
            ROW_NUMBER() OVER (
                PARTITION BY rt_source, school_id_giga
                ORDER BY created_timestamp DESC
            ) AS row_num,
            rt_source
        FROM default.all_gigameter_measurement_data_tb_physical
    ) AS a
    WHERE row_num = 1  -- Keep only the most recent measurement
),


-- ==============================================================================
-- CTE: max_app_version_per_school
-- Purpose: Gets the highest semantic app version across all devices for a school
-- Source:  default.all_gigameter_measurement_data_tb_physical
-- Method:  Parse version into major.minor.patch and select max by semantic order
-- Output:  school_id_giga, rt_source, max_app_version
-- CAVEAT:  Assumes version format is X.Y.Z (major.minor.patch)
-- ==============================================================================
max_app_version_per_school AS (
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
        FROM default.all_gigameter_measurement_data_tb_physical
        WHERE app_version IS NOT NULL
    )
    WHERE version_rank = 1
),


-- ==============================================================================
-- CTE: devices_per_school
-- Purpose: Counts distinct devices per school from measurement data
-- Source:  default.all_gigameter_measurement_data_tb_physical
-- Output:  school_id_giga, rt_source, num_devices
-- ==============================================================================
devices_per_school AS (
    SELECT
        school_id_giga,
        rt_source,
        COUNT(DISTINCT device_id) AS num_devices
    FROM default.all_gigameter_measurement_data_tb_physical
    WHERE device_id IS NOT NULL
    GROUP BY school_id_giga, rt_source
),


-- ==============================================================================
-- CTE: registration_attempts
-- Purpose: Counts registration activity metrics per school
-- Source:  gigameter_production_db.public.dailycheckapp_school
-- Output:  Device counts, user IDs, registration attempts
-- CAVEAT:  Only tracks DailyCheckApp (GigaMeter) registrations, not MLAB
-- ==============================================================================
registration_attempts AS (
    SELECT
        giga_id_school AS school_id_giga,
        COUNT(DISTINCT mac_address) AS num_user_ids,
        COUNT(DISTINCT created) AS num_registration_attempts,
        COUNT(DISTINCT device_hardware_id) AS num_devices_registered,
        COUNT(DISTINCT windows_username) AS num_windows_usernames,
        'DailyCheckApp' AS rt_source
    FROM gigameter_production_db.public.dailycheckapp_school
    GROUP BY giga_id_school
),


-- ==============================================================================
-- CTE: primary_source
-- Purpose: Determines the primary and secondary data source per school
-- Method:  approx_most_frequent() to find most common rt_source
-- Output:  primary_source (most frequent), secondary_source (2nd most frequent)
-- CAVEAT:  Uses approximate mode, not exact counts
-- ==============================================================================
primary_source AS (
    SELECT DISTINCT
        country,
        school_id_giga,
        school_id_govt,
        element_at(map_keys(source), 1) AS primary_source,
        element_at(map_keys(source), 2) AS secondary_source
    FROM (
        SELECT
            country,
            school_id_giga,
            school_id_govt,
            approx_most_frequent(5, rt_source, 5) AS source
        FROM (
            SELECT
                country,
                school_id_giga,
                school_id_govt,
                rt_source
            FROM default.all_gigameter_measurement_data_tb_physical
        ) AS c
        GROUP BY country, school_id_giga, school_id_govt
    ) AS d
),


-- ==============================================================================
-- CTE: connectivity_stats
-- Purpose: Gets GigaMaps connectivity status for schools
-- Source:  gigamaps_production_db.public.connection_statistics_schoolweeklystatus
-- Output:  connectivity_gigamaps (connected/not connected/other)
-- CAVEAT:  Currently filtered to Bosnia and Herzegovina only
-- ==============================================================================
connectivity_stats AS (
    SELECT DISTINCT
        c.name AS country,
        school.giga_id_school AS school_id_giga,
        school.name AS school_name,
        -- Map connectivity status to simplified categories
        CASE
            WHEN connectivity_status IN ('good', 'moderate') THEN 'connected'
            WHEN connectivity = false AND connectivity_status = 'no' THEN 'not connected'
            WHEN connectivity IS NULL AND connectivity_status = 'unknown' THEN 'other'
            ELSE NULL
        END AS connectivity_gigamaps
    FROM gigamaps_production_db.public.connection_statistics_schoolweeklystatus conn_stats
    INNER JOIN gigamaps_production_db.public.schools_school AS school
        ON school.id = conn_stats.school_id
        AND school.last_weekly_status_id = conn_stats.id
    LEFT JOIN gigamaps_production_db.public.locations_country AS c
        ON c.id = school.country_id
    WHERE school.deleted IS NULL
        --AND c.name = 'Bosnia and Herzegovina'  -- Bosnia-specific filter
),


-- ==============================================================================
-- CTE: primary_source_per_school
-- Purpose: Links each school with its primary data source
-- Source:  all_gigameter_measurement_data_tb_physical + primary_source
-- Output:  Unique school records with their primary/secondary sources
-- ==============================================================================
primary_source_per_school AS (
    SELECT DISTINCT
        data.country,
        data.iso3_format,
        data.school_id_giga,
        data.school_id_govt,
        ps.primary_source,
        ps.secondary_source
    FROM default.all_gigameter_measurement_data_tb_physical data
    INNER JOIN primary_source AS ps
        ON data.school_id_giga = ps.school_id_giga
    GROUP BY
        data.country,
        data.iso3_format,
        data.school_id_giga,
        data.school_id_govt,
        ps.primary_source,
        ps.secondary_source
),


-- ==============================================================================
-- CTE: measurement_data
-- Purpose: Aggregates measurement statistics per school
-- Source:  primary_source_per_school + all_gigameter_measurement_data_tb_physical
-- Output:  First measurement date, measurement counts, detected ISP/server
-- CAVEAT:  approx_most_frequent() used for ISP/server mode detection
-- ==============================================================================
measurement_data AS (
    SELECT DISTINCT
        am.country,
        am.iso3_format,
        am.school_id_giga,
        am.school_id_govt,
        am.primary_source,
        am.secondary_source,
        -- Mode detection for server and ISP
        approx_most_frequent(5, m_server.detected_server, 5) AS detected_server_mode,
        approx_most_frequent(5, m_server.isp_clean, 5) AS detected_isp,
        approx_most_frequent(5, m_server.isp_asn_clean, 5) AS detected_isp_asn,
        -- Measurement statistics
        MIN(m_server.local_created_timestamp) AS first_measurement_date,
        COUNT(DISTINCT m_server.local_created_timestamp) AS num_measurements,
        COUNT(DISTINCT DATE_TRUNC('day', m_server.local_created_timestamp)) AS num_days_measured
    FROM primary_source_per_school am
    LEFT JOIN default.all_gigameter_measurement_data_tb_physical m_server
        ON am.school_id_govt = m_server.school_id_govt
        AND am.country = m_server.country
    GROUP BY
        am.country,
        am.iso3_format,
        am.school_id_giga,
        am.school_id_govt,
        am.primary_source,
        am.secondary_source
),


-- ==============================================================================
-- CTE: app_versions
-- Purpose: Gets app version history from funnel tracking table
-- Source:  default.all_gigameter_appversion_funnel
-- Output:  Initial/most recent app versions, initial registration date
-- ==============================================================================
app_versions AS (
    SELECT
        school_id_govt,
        school_id_giga,
        MIN(ap.initial_registration_date) AS initial_registration_date,
        MAX(ap.most_recent_app_version_clean) AS most_recent_app_version_clean,
        MIN(ap.initial_app_version_clean) AS initial_app_version_clean,
        MIN(initial_rt_source) AS initial_rt_source
    FROM default.all_gigameter_appversion_funnel ap
    GROUP BY school_id_govt, school_id_giga
),


-- ==============================================================================
-- CTE: pre_table
-- Purpose: Combines measurement data with registration and version info
-- JOINs:
--   - all_gigameter_schools_initial_registration_date_vw: fallback registration dates
--   - app_versions: app version tracking
--   - last_measurement_date: most recent measurement info
--   - registration_attempts: device/user counts
-- ==============================================================================
pre_table AS (
    SELECT DISTINCT
        measure.iso3_format,
        measure.country,
        measure.school_id_giga,
        measure.school_id_govt,
        -- Use app_versions registration date, fallback to initial registration view
        COALESCE(ap.initial_registration_date, ir.initial_registration_date) AS initial_registration_date,
        ap.most_recent_app_version_clean AS most_recent_app_version,
        ap.initial_app_version_clean AS initial_app_version,
        measure.primary_source,
        measure.secondary_source,
        measure.first_measurement_date,
        -- Calculate days since last measurement for status determination
        EXTRACT(DAY FROM CURRENT_DATE - lm.last_measurement_date) AS days_since_last_measurement,
        measure.num_measurements,
        measure.num_days_measured,
        lm.last_measurement_date,
        lm.last_app_version AS latest_measurement_app_version,
        mav.max_app_version AS highest_app_version_in_use,
        lm.rt_source AS last_measurement_source,
        COALESCE(dps.num_devices, 0) AS num_devices_registered,
        ra.num_user_ids,
        ra.num_windows_usernames,
        ra.num_registration_attempts,
        -- Extract primary/secondary server and ISP from mode detection
        element_at(map_keys(measure.detected_server_mode), 1) AS primary_server,
        element_at(map_keys(measure.detected_server_mode), 2) AS secondary_server,
        element_at(map_keys(measure.detected_isp), 1) AS primary_isp,
        element_at(map_keys(measure.detected_isp_asn), 1) AS primary_isp_asn,
        element_at(map_keys(measure.detected_isp), 2) AS secondary_isp,
        element_at(map_keys(measure.detected_isp_asn), 2) AS secondary_isp_asn
    FROM measurement_data measure
    LEFT JOIN default.all_gigameter_schools_initial_registration_date_vw ir
        ON LOWER(measure.school_id_giga) = LOWER(ir.school_id_giga)
        AND measure.primary_source = ir.rt_source
    LEFT JOIN app_versions ap
        ON measure.school_id_giga = ap.school_id_giga
        AND measure.primary_source = ap.initial_rt_source
    LEFT JOIN last_measurement_date lm
        ON measure.school_id_giga = lm.school_id_giga
        AND measure.primary_source = lm.rt_source
    LEFT JOIN registration_attempts AS ra
        ON ra.school_id_giga = measure.school_id_giga
    LEFT JOIN max_app_version_per_school mav
        ON measure.school_id_giga = mav.school_id_giga
        AND measure.primary_source = mav.rt_source
    LEFT JOIN devices_per_school dps
        ON measure.school_id_giga = dps.school_id_giga
        AND measure.primary_source = dps.rt_source
),


-- ==============================================================================
-- CTE: vt (virtual table)
-- Purpose: Final enrichment with school master data and status calculations
-- JOINs:
--   - FULL JOIN all_school_master: includes all schools even without measurements
--   - all_gigameter_schools_funnel_status_vw: funnel status tracking
--   - Bosnia canton mapping and connectivity credits (country-specific)
-- Status Fields:
--   - registered_gigameter: Yes/No based on initial_registration_date
--   - installed_gigameter: Yes/No based on initial_app_version
--   - sending_gigameter_data: Yes/No based on first_measurement_date
--   - install_status: live/at-risk/drop-off based on days since last measurement
-- ==============================================================================
vt AS (
    SELECT DISTINCT
        COALESCE(pt.iso3_format, master.iso3_code) AS iso3_code,
        COALESCE(pt.country, master.country) AS country,
        COALESCE(pt.school_id_giga, master.school_id_giga) AS school_id_giga,
        COALESCE(pt.school_id_govt, master.school_id_govt) AS school_id_govt,
        master.school_name,
        pt.primary_source,
        pt.secondary_source,
        pt.initial_registration_date,
        pt.initial_app_version,
        pt.most_recent_app_version,
        pt.first_measurement_date,
        pt.last_measurement_date,
        pt.latest_measurement_app_version,
        pt.highest_app_version_in_use,
        pt.last_measurement_source,
        pt.days_since_last_measurement,
        pt.num_measurements,
        pt.num_days_measured,
        pt.num_devices_registered,
        pt.num_user_ids,
        pt.num_windows_usernames,
        pt.num_registration_attempts,
        pt.primary_server,
        pt.secondary_server,
        pt.primary_isp,
        pt.primary_isp_asn,
        pt.secondary_isp,
        pt.secondary_isp_asn,

        -- School master enrichment fields
        master.admin1,
        master.admin2,
        master.admin3,
        master.admin4,
        master.connectivity,
        master.connectivity_type_govt,
        master.cellular_coverage_type,
        master.school_area_type,
        master.school_funding_type,
        master.education_level,
        master.electricity_availability,
        master.fiber_node_distance,
        master.latitude,
        master.longitude,
        master.unicef_region,
        master.continent,
        master.itu_region,
        master.disputed_region,
        master.num_students,

        -- Funnel status fields
        status.gigameter_funnel_status,
        status.gigameter_funnel_status_order,
        status.detected_devices,

        -- =====================================================================
        -- Derived Status Fields
        -- These flags indicate school progress through the GigaMeter funnel
        -- =====================================================================
        CASE
            WHEN pt.initial_registration_date IS NULL THEN 'No'
            ELSE 'Yes'
        END AS registered_gigameter,

        CASE
            WHEN pt.initial_app_version IS NULL THEN 'No'
            ELSE 'Yes'
        END AS installed_gigameter,

        CASE
            WHEN pt.first_measurement_date IS NULL THEN 'No'
            ELSE 'Yes'
        END AS sending_gigameter_data,

        -- Install status based on recency of measurements
        -- CAVEAT: Thresholds are 21 days (at-risk) and 30 days (drop-off)
        CASE
            WHEN pt.days_since_last_measurement <= 21 THEN 'live'
            WHEN pt.days_since_last_measurement BETWEEN 21 AND 30 THEN 'at-risk'
            WHEN pt.days_since_last_measurement >= 30 THEN 'drop-off'
            ELSE 'Unknown'
        END AS install_status,

        -- Country-specific fields
        c.canton_name AS canton_bih_only,
        cc.status AS connectivity_credits_school,
        cs.connectivity_gigamaps,
        p.zaf_province as province_zaf_only

    FROM pre_table pt
    -- FULL JOIN to include all schools, even those without measurements
    FULL JOIN default.all_school_master master
        ON master.school_id_giga = pt.school_id_giga
    LEFT JOIN default.all_gigameter_schools_funnel_status_vw status
        ON master.school_id_giga = status.school_id_giga
    LEFT JOIN lstringer.bih_school_canton_mapping_vw c
        ON master.school_id_giga = c.school_id_giga
    LEFT JOIN lstringer.connectivity_credit_pilot_schools cc
        ON master.school_id_giga = cc.school_id_giga
    LEFT JOIN connectivity_stats cs
        ON master.school_id_giga = cs.school_id_giga

    LEFT JOIN lstringer.zaf_province_mapping p
      on master.school_id_giga = p.school_id_giga
)


-- ==============================================================================
-- FINAL SELECT
-- Purpose: Output final columns with clean naming
-- Transform: Renames 'DailyCheckApp' to 'GigaMeter' for clarity
-- ==============================================================================
SELECT
    iso3_code,
    country,
    school_id_giga,
    school_id_govt,
    school_name,
    -- Rename internal source names to user-friendly names
    CASE WHEN primary_source = 'DailyCheckApp' THEN 'GigaMeter' ELSE primary_source END AS primary_source,
    CASE WHEN secondary_source = 'DailyCheckApp' THEN 'GigaMeter' ELSE secondary_source END AS secondary_source,
    initial_registration_date,
    initial_app_version,
    most_recent_app_version,
    first_measurement_date,
    last_measurement_date,
    latest_measurement_app_version,
    highest_app_version_in_use,
    last_measurement_source,
    days_since_last_measurement,
    num_measurements,
    num_days_measured,
    num_devices_registered,
    detected_devices AS num_devices_detected,
    num_user_ids,
    num_windows_usernames,
    num_registration_attempts,
    primary_server,
    secondary_server,
    primary_isp,
    primary_isp_asn,
    secondary_isp,
    secondary_isp_asn,
    -- School master fields
    admin1,
    admin2,
    admin3,
    admin4,
    connectivity,
    connectivity_type_govt,
    cellular_coverage_type,
    school_area_type,
    school_funding_type,
    education_level,
    electricity_availability,
    fiber_node_distance,
    latitude,
    longitude,
    unicef_region,
    continent,
    itu_region,
    disputed_region,
    num_students,
    -- Status fields
    registered_gigameter,
    installed_gigameter,
    sending_gigameter_data,
    install_status,
    gigameter_funnel_status,
    gigameter_funnel_status_order,
    -- Country-specific fields
    canton_bih_only,
    connectivity_credits_school,
    connectivity_gigamaps,
    province_zaf_only

FROM vt

 );
