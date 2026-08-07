

-- Script Name:     all_school_master.sql
-- Table Created:   default.all_school_master
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Master reference table containing all schools across all countries in the
--   Giga database. Consolidates school metadata from the school_master_all
--   source with country geography and version tracking information. This is
--   the foundational table that other Giga Meter tables join against.
--
-- Dependencies:
--   - custom_dataset.school_master_all (primary source of school data)
--   - custom_dataset.country_geography (country names, regions, continents)
--   - gigamaps_production_db.public.locations_country (ISO3 country codes)
--   - default.country_versions (school master version tracking)
--
-- Output Columns:  69 columns
-- Primary Key:     school_id_giga (36-character hash)
-- Approximate Rows: ~2.1M schools (all countries)
--
-- CAVEATS:
--   - Some countries (TCA, NAM, SWZ, MNE, DJI, URY, AGO) require manual name
--     overrides because they are missing from the country_geography lookup
--   - connectivity vs connectivity_govt: 'connectivity' is Giga-standardized,
--     'connectivity_govt' is original government-reported value
--   - education_level vs education_level_govt: similar standardization applies
--   - connectivity_rt fields are from real-time measurement data (MLAB, QoS, GigaMeter)
--   - school_master_latest_version/last_updated track delta table versions
--   - License: Mix of ODBL (open) and Giga (proprietary) licensed fields
--
-- Last Updated:    2025-10-31 / Luke Stringer
-- ==============================================================================


-- ==============================================================================
-- SESSION CONFIGURATION
-- ==============================================================================
-- mark_distinct_strategy = 'none': Disables Presto's automatic distinct
--   optimization to prevent the query planner from choosing suboptimal
--   strategies for this complex multi-CTE query
-- query_max_stage_count = 300: Increases the maximum number of query stages
--   allowed, necessary for complex queries with multiple CTEs and JOINs
-- ==============================================================================
SET SESSION mark_distinct_strategy = 'none';
SET SESSION query_max_stage_count = 300;




CREATE TABLE IF NOT EXISTS default.all_school_master
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_school_master'
)
AS (


-- ==============================================================================
-- CTE: country_geography
-- Purpose: Builds a lookup table mapping ISO2 codes to country metadata
-- Source:  custom_dataset.country_geography (primary) + gigamaps locations
-- Output:  Country name, ISO2/ISO3 codes, UNICEF region, ITU region, continent
-- Caveats: Some countries may be missing - handled via CASE statements in main query
-- ==============================================================================
WITH country_geography AS (
    SELECT
        g.Country,
        g.Code AS iso2_code,
        lc.iso3_format AS iso3_code,
        g.unicef_region,
        g.itu_region,
        g.continent
    FROM
        custom_dataset.country_geography g
    LEFT JOIN
        gigamaps_production_db.public.locations_country lc
    ON
        g.code = lc.code
),


-- ==============================================================================
-- CTE: versions
-- Purpose: Gets the latest version number and update date for each country's
--          school master delta table
-- Source:  default.country_versions (tracks delta_lake.school_master versions)
-- Output:  country_code, school_master_latest_version, school_master_last_updated
-- Caveats: Ensures we track which version of source data is being used
-- ==============================================================================
versions AS (
    SELECT
        DISTINCT
        UPPER(country_code) AS country_code,
        max_version AS school_master_latest_version,
        max_date AS school_master_last_updated,
        min_date as school_master_first_updated
    FROM
        default.country_versions
)


-- ==============================================================================
-- MAIN SELECT
-- Purpose: Combines school data with geography and version info
-- Source:  custom_dataset.school_master_all (consolidated school data)
-- JOINs:
--   - LEFT JOIN country_geography: Adds regional classification (some countries missing)
--   - LEFT JOIN versions: Adds version tracking metadata
-- ==============================================================================

SELECT
    -- -------------------------------------------------------------------------
    -- Geographic Classification Fields
    -- CAVEAT: Manual overrides for countries missing from country_geography
    -- TCA = Turks and Caicos, NAM = Namibia, SWZ = Eswatini, MNE = Montenegro
    -- DJI = Djibouti, URY = Uruguay, AGO = Angola
    -- -------------------------------------------------------------------------
    -- -------------------------------------------------------------------------
    -- Geographic Classification Fields
    -- CAVEAT: Manual overrides for countries missing from country_geography
    -- TCA = Turks and Caicos, NAM = Namibia, SWZ = Eswatini, MNE = Montenegro
    -- DJI = Djibouti, URY = Uruguay, AGO = Angola
    -- -------------------------------------------------------------------------
    CAST(CASE
        WHEN master.country = 'TCA' THEN 'Turks and Caicos Islands'
        WHEN master.country = 'NAM' THEN 'Namibia'
        WHEN master.country = 'SWZ' THEN 'Eswatini'
        WHEN master.country = 'MNE' THEN 'Montenegro'
        WHEN master.country = 'DJI' THEN 'Djibouti'
        WHEN master.country = 'URY' THEN 'Uruguay'
        WHEN master.country = 'AGO' THEN 'Angola'
        WHEN master.country = 'ALB' THEN 'Albania'
        ELSE geo.country
    END AS VARCHAR) AS country,             -- Full country name (with manual overrides)

    CAST(CASE
        WHEN master.country IN ('TCA', 'URY') THEN 'Latin America and Caribbean'
        WHEN master.country IN ('NAM', 'SWZ', 'DJI', 'AGO') THEN 'Eastern and Southern Africa'
        WHEN master.country IN ('MNE', 'ALB') THEN 'Eastern Europe and Central Asia'
        ELSE geo.unicef_region
    END AS VARCHAR) AS unicef_region,       -- UNICEF regional classification

    CAST(CASE
        WHEN master.country IN ('TCA', 'URY') THEN 'The Americas'
        WHEN master.country IN ('NAM', 'SWZ', 'DJI', 'AGO') THEN 'Africa'
        WHEN master.country IN ('MNE', 'ALB') THEN 'Europe'
        ELSE geo.itu_region
    END AS VARCHAR) AS itu_region,          -- ITU regional classification

    CAST(CASE
        WHEN master.country IN ('TCA', 'URY') THEN 'South America'
        WHEN master.country IN ('NAM', 'SWZ', 'DJI', 'AGO') THEN 'Africa'
        WHEN master.country IN ('MNE', 'ALB') THEN 'Europe'
        ELSE geo.continent
    END AS VARCHAR) AS continent,           -- Geographic continent

    -- -------------------------------------------------------------------------
    -- School Identity Fields
    -- -------------------------------------------------------------------------
    CAST(master.country AS VARCHAR) AS iso3_code,            -- ISO 3166-1 alpha-3 country code
    CAST(master.school_id_giga AS VARCHAR) AS school_id_giga, -- Unique Giga identifier (36-char hash) - PRIMARY KEY
    CAST(master.school_id_govt AS VARCHAR) AS school_id_govt, -- Government-assigned school ID (format varies by country)
    CAST(master.school_name AS VARCHAR) AS school_name,      -- Official school name
    CAST(master.school_establishment_year AS DOUBLE) AS school_establishment_year, -- Year school was established (min=1000, max=current)

    -- -------------------------------------------------------------------------
    -- Location Fields
    -- -------------------------------------------------------------------------
    CAST(master.latitude AS DOUBLE) AS latitude,             -- School latitude coordinate
    CAST(master.longitude AS DOUBLE) AS longitude,           -- School longitude coordinate

    -- -------------------------------------------------------------------------
    -- Education Classification
    -- CAVEAT: education_level is Giga-standardized; education_level_govt is original
    -- Valid values: Pre-Primary, Primary, Secondary, Post-Secondary (or combinations)
    -- -------------------------------------------------------------------------
    CAST(master.education_level AS VARCHAR) AS education_level, -- Giga-standardized education level

    -- -------------------------------------------------------------------------
    -- Connectivity Fields (Government-reported)
    -- CAVEAT: These are government-reported values, not real-time measurements
    -- connectivity_govt vs connectivity: govt is original, connectivity is standardized
    -- -------------------------------------------------------------------------
    CAST(master.download_speed_contracted AS DOUBLE) AS download_speed_contracted, -- Contracted download speed (0-500 Mbps)
    CAST(master.connectivity_type_govt AS VARCHAR) AS connectivity_type_govt,      -- Government-reported connection type

    -- -------------------------------------------------------------------------
    -- Administrative Boundaries
    -- admin1 = State/Province, admin2 = District/County, admin3/4 = lower levels
    -- -------------------------------------------------------------------------
    CAST(master.admin1 AS VARCHAR) AS admin1,                -- Admin level 1 name (state/province)
    CAST(master.admin1_id_giga AS VARCHAR) AS admin1_id_giga, -- Giga ID for admin1
    CAST(master.admin2 AS VARCHAR) AS admin2,                -- Admin level 2 name (district/county)
    CAST(master.admin2_id_giga AS VARCHAR) AS admin2_id_giga, -- Giga ID for admin2
    CAST(master.admin3 AS VARCHAR) AS admin3,                -- Admin level 3 name
    CAST(master.admin3_id_giga AS VARCHAR) AS admin3_id_giga, -- Giga ID for admin3
    CAST(master.admin4 AS VARCHAR) AS admin4,                -- Admin level 4 name
    CAST(master.admin4_id_giga AS VARCHAR) AS admin4_id_giga, -- Giga ID for admin4

    -- -------------------------------------------------------------------------
    -- School Classification
    -- -------------------------------------------------------------------------
    CAST(master.school_area_type AS VARCHAR) AS school_area_type,     -- urban/rural classification
    CAST(master.school_funding_type AS VARCHAR) AS school_funding_type, -- Funding type (public/private/etc)

    -- -------------------------------------------------------------------------
    -- Data Source Metadata
    -- CAVEAT: Important for understanding data freshness and reliability
    -- -------------------------------------------------------------------------
    CAST(master.school_data_source AS VARCHAR) AS school_data_source,             -- Entity that provided the data
    CAST(master.school_data_collection_year AS DOUBLE) AS school_data_collection_year, -- Year data was collected
    CAST(master.school_data_collection_modality AS VARCHAR) AS school_data_collection_modality, -- How data was collected (survey, admin, etc)

    -- -------------------------------------------------------------------------
    -- Cellular/Mobile Infrastructure
    -- -------------------------------------------------------------------------
    CAST(master.cellular_coverage_availability AS VARCHAR) AS cellular_coverage_availability, -- Whether cellular coverage exists
    CAST(master.cellular_coverage_type AS VARCHAR) AS cellular_coverage_type,     -- Type of cellular coverage (2G/3G/4G/5G)

    -- -------------------------------------------------------------------------
    -- Distance to Infrastructure (in km)
    -- -------------------------------------------------------------------------
    CAST(master.fiber_node_distance AS DOUBLE) AS fiber_node_distance,            -- Distance to nearest fiber node
    CAST(master.microwave_node_distance AS DOUBLE) AS microwave_node_distance,    -- Distance to nearest microwave node

    -- -------------------------------------------------------------------------
    -- Proximity Metrics
    -- CAVEAT: Used for duplicate detection and density analysis
    -- -------------------------------------------------------------------------
    CAST(master.schools_within_1km AS DOUBLE) AS schools_within_1km,              -- Count of other schools within 1km
    CAST(master.schools_within_2km AS DOUBLE) AS schools_within_2km,              -- Count of other schools within 2km
    CAST(master.schools_within_3km AS DOUBLE) AS schools_within_3km,              -- Count of other schools within 3km

    -- -------------------------------------------------------------------------
    -- Distance to Cellular Towers (in km)
    -- NR = 5G (New Radio), LTE = 4G, UMTS = 3G, GSM = 2G
    -- -------------------------------------------------------------------------
    CAST(master.nearest_nr_distance AS DOUBLE) AS nearest_nr_distance,            -- Distance to nearest 5G tower
    CAST(master.nearest_lte_distance AS DOUBLE) AS nearest_lte_distance,          -- Distance to nearest 4G/LTE tower
    CAST(master.nearest_umts_distance AS DOUBLE) AS nearest_umts_distance,        -- Distance to nearest 3G tower
    CAST(master.nearest_gsm_distance AS DOUBLE) AS nearest_gsm_distance,          -- Distance to nearest 2G tower

    -- -------------------------------------------------------------------------
    -- Population Within Radius
    -- CAVEAT: Used for num_students_discrepancy flag (if num_students > pop_within_1km)
    -- -------------------------------------------------------------------------
    CAST(master.pop_within_1km AS DOUBLE) AS pop_within_1km,                      -- Population within 1km radius
    CAST(master.pop_within_2km AS DOUBLE) AS pop_within_2km,                      -- Population within 2km radius
    CAST(master.pop_within_3km AS DOUBLE) AS pop_within_3km,                      -- Population within 3km radius

    -- -------------------------------------------------------------------------
    -- Government Connectivity Data
    -- CAVEAT: connectivity_govt_collection_year indicates data freshness
    -- -------------------------------------------------------------------------
    CAST(master.connectivity_govt_collection_year AS DOUBLE) AS connectivity_govt_collection_year, -- Year government connectivity data collected
    CAST(master.connectivity_govt AS VARCHAR) AS connectivity_govt,                -- Government-reported connectivity status
    CAST(master.connectivity_govt_ingestion_timestamp AS VARCHAR) AS connectivity_govt_ingestion_timestamp, -- When govt connectivity data was ingested

    -- -------------------------------------------------------------------------
    -- Data Ingestion Timestamps
    -- -------------------------------------------------------------------------
    CAST(master.school_location_ingestion_timestamp AS VARCHAR) AS school_location_ingestion_timestamp, -- When school location was added to Giga

    -- -------------------------------------------------------------------------
    -- Special Classifications
    -- -------------------------------------------------------------------------
    CAST(master.disputed_region AS VARCHAR) AS disputed_region,                   -- Whether school is in disputed territory

    -- -------------------------------------------------------------------------
    -- Giga-Standardized Connectivity
    -- CAVEAT: 'connectivity' is Giga-standardized from multiple sources
    -- -------------------------------------------------------------------------
    CAST(master.connectivity AS VARCHAR) AS connectivity,                          -- Giga-standardized connectivity status

    -- -------------------------------------------------------------------------
    -- Real-Time Connectivity Data
    -- CAVEAT: From live measurements (MLAB, QoS API, GigaMeter app)
    -- connectivity_rt_datasource: mlab, QoS, gigameter
    -- -------------------------------------------------------------------------
    CAST(master.connectivity_rt AS VARCHAR) AS connectivity_rt,                    -- Real-time connectivity status (Yes if >=1 measurement)
    CAST(master.connectivity_rt_datasource AS VARCHAR) AS connectivity_rt_datasource, -- Source of RT data (mlab, QoS, gigameter)
    CAST(master.connectivity_rt_ingestion_timestamp AS VARCHAR) AS connectivity_rt_ingestion_timestamp, -- When RT connectivity data was ingested

    -- -------------------------------------------------------------------------
    -- School Identifier Signature
    -- -------------------------------------------------------------------------
    CAST(master.signature AS VARCHAR) AS signature,                                -- Hash/signature for record identification

    -- -------------------------------------------------------------------------
    -- School Resources
    -- -------------------------------------------------------------------------
    CAST(master.num_computers AS DOUBLE) AS num_computers,                         -- Current number of computers
    CAST(master.num_computers_desired AS DOUBLE) AS num_computers_desired,         -- Desired/needed number of computers
    CAST(master.num_teachers AS DOUBLE) AS num_teachers,                           -- Total number of teachers
    CAST(master.num_adm_personnel AS DOUBLE) AS num_adm_personnel,                 -- Number of administrative personnel
    CAST(master.num_students AS DOUBLE) AS num_students,                           -- Total student enrollment
    CAST(master.num_classrooms AS DOUBLE) AS num_classrooms,                       -- Number of classrooms
    CAST(master.num_latrines AS DOUBLE) AS num_latrines,                           -- Number of latrines/toilets
    CAST(master.computer_lab AS VARCHAR) AS computer_lab,                          -- Whether school has a computer lab

    -- -------------------------------------------------------------------------
    -- Utilities
    -- CAVEAT: Used for electricity_discrepancy flag calculation
    -- -------------------------------------------------------------------------
    CAST(master.electricity_availability AS VARCHAR) AS electricity_availability,  -- Whether electricity is available
    CAST(master.electricity_type AS VARCHAR) AS electricity_type,                  -- Type of electricity (grid/solar/generator)
    CAST(master.water_availability AS VARCHAR) AS water_availability,              -- Whether water is available

    -- -------------------------------------------------------------------------
    -- Connectivity Benchmarks
    -- -------------------------------------------------------------------------
    CAST(master.download_speed_benchmark AS DOUBLE) AS download_speed_benchmark,   -- Target download speed for school

    -- -------------------------------------------------------------------------
    -- Student Demographics
    -- -------------------------------------------------------------------------
    CAST(master.num_students_boys AS DOUBLE) AS num_students_boys,                 -- Number of male students
    CAST(master.num_students_girls AS DOUBLE) AS num_students_girls,               -- Number of female students

    -- -------------------------------------------------------------------------
    -- Technology Resources
    -- -------------------------------------------------------------------------
    CAST(master.computer_availability AS VARCHAR) AS computer_availability,        -- Computer availability status
    CAST(master.teachers_trained AS VARCHAR) AS teachers_trained,                   -- Number of teachers trained in ICT

    -- -------------------------------------------------------------------------
    -- Building Information
    -- -------------------------------------------------------------------------
    CAST(master.building_id_govt AS DOUBLE) AS building_id_govt,                  -- Government building identifier
    CAST(master.num_schools_per_building AS DOUBLE) AS num_schools_per_building,   -- Number of schools sharing building

    -- -------------------------------------------------------------------------
    -- Teacher Demographics
    -- -------------------------------------------------------------------------
    CAST(master.num_teachers_male AS DOUBLE) AS num_teachers_male,                 -- Number of male teachers
    CAST(master.num_teachers_female AS DOUBLE) AS num_teachers_female,             -- Number of female teachers

    -- -------------------------------------------------------------------------
    -- Device Resources
    -- -------------------------------------------------------------------------
    CAST(master.device_availability AS VARCHAR) AS device_availability,            -- Device availability status
    CAST(master.num_tablets AS DOUBLE) AS num_tablets,                             -- Number of tablets
    CAST(master.num_robotic_equipment AS DOUBLE) AS num_robotic_equipment,         -- Number of robotic equipment pieces

    -- -------------------------------------------------------------------------
    -- Sustainability
    -- -------------------------------------------------------------------------
    CAST(master.sustainable_business_model AS VARCHAR) AS sustainable_business_model, -- Whether school has sustainable model

    -- -------------------------------------------------------------------------
    -- Location Source
    -- -------------------------------------------------------------------------
    CAST(master.source_lat_lon AS VARCHAR) AS source_lat_lon,                      -- Source of latitude/longitude coordinates

    -- -------------------------------------------------------------------------
    -- Connectivity Type (Giga-standardized)
    -- Valid values: fiber, copper, coaxial, wired_other, unknown_wired,
    --               cellular, p2p, satellite, haps, drones, unknown_wireless,
    --               other_wireless, unknown
    -- -------------------------------------------------------------------------
    CAST(master.connectivity_type AS VARCHAR) AS connectivity_type,                -- Giga-standardized connectivity type
    CAST(master.connectivity_type_root AS VARCHAR) AS connectivity_type_root,      -- Root/primary connectivity type
    CAST(master.connected_node_dist AS DOUBLE) AS connected_node_dist,             -- Distance to connected infrastructure node

    -- -------------------------------------------------------------------------
    -- Points of Interest Analysis
    -- -------------------------------------------------------------------------
    CAST(master.poi_count_1km AS VARCHAR) AS poi_count_1km,                         -- POI count within 1km
    CAST(master.poi_count_3km AS VARCHAR) AS poi_count_3km,                         -- POI count within 3km
    CAST(master.poi_count_5km AS VARCHAR) AS poi_count_5km,                         -- POI count within 5km

    -- -------------------------------------------------------------------------
    -- Population (Extended Radius)
    -- -------------------------------------------------------------------------
    CAST(master.pop_within_5km AS VARCHAR) AS pop_within_5km,                       -- Population within 5km radius

    -- -------------------------------------------------------------------------
    -- Cell Site Visibility Analysis
    -- -------------------------------------------------------------------------
    CAST(master.is_visible AS VARCHAR) AS is_visible,                              -- Whether school is visible from cell sites
    CAST(master.visible_cell_site_dist AS VARCHAR) AS visible_cell_site_dist,       -- Distance to visible cell site
    CAST(master.visible_cell_site_radio_type AS VARCHAR) AS visible_cell_site_radio_type, -- Radio type of visible cell site

    -- -------------------------------------------------------------------------
    -- Education Level (Government-reported)
    -- CAVEAT: education_level_govt is original, education_level is standardized
    -- -------------------------------------------------------------------------
    CAST(master.education_level_govt AS VARCHAR) AS education_level_govt,          -- Government-reported education level

    -- -------------------------------------------------------------------------
    -- Version Tracking
    -- CAVEAT: Tracks which version of delta_lake.school_master was used
    -- -------------------------------------------------------------------------
    CAST(versions.school_master_latest_version AS BIGINT) AS school_master_latest_version, -- Version number of school master data
    CAST(versions.school_master_last_updated AS TIMESTAMP) AS school_master_last_updated,   -- Last update date of school master data
    CAST(versions.school_master_first_updated AS TIMESTAMP) AS school_master_first_updated  -- first update date of school master data

FROM
    custom_dataset.school_master_all master

-- JOIN: Add country geography (regional classification)
-- Type: LEFT JOIN - preserves all schools even if country not in geography lookup
LEFT JOIN
    country_geography geo
ON
    master.country = geo.iso3_code

-- JOIN: Add version tracking information
-- Type: LEFT JOIN - preserves all schools even without version info
LEFT JOIN
    versions
ON
    master.country = versions.country_code



);
