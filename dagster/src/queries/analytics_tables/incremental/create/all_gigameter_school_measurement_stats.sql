-- ==============================================================================
-- Script Name:     all_gigameter_school_measurement_stats.sql (incremental/create)
-- Table Created:   default.all_gigameter_school_measurement_stats
--
-- Purpose:
--   INTERNAL accumulator (not for dashboards) holding lifetime measurement
--   aggregates per school, so default.all_gigameter_registered_schools can be
--   rebuilt hourly from a cheap join instead of re-scanning the full
--   all_gigameter_measurement_data history three times.
--
-- Granularity:     One row per NON-NULL school_id_giga that has ever measured.
--                  Unpartitioned (small).
--
-- Columns:
--   first_measurement_date / last_measurement_date
--                    MIN / MAX of local_created_timestamp (school-local time)
--   max_app_version_gigameter (+ _rank)
--                    highest app_version among rt_source = 'GigaMeter' rows,
--                    ranked major*1e6 + minor*1e3 + patch (TRY_CAST parts,
--                    unparseable parts count as 0)
--   device_ids       distinct non-null device_id values
--   source_counts / isp_counts / isp_asn_counts / server_counts
--                    EXACT per-value counts (histogram ignores NULLs) of
--                    rt_source / isp_name / isp_asn / detected_server -- exact
--                    counts combine across runs, approx_most_frequent output
--                    does not. Top 10 is picked when registered_schools is built.
--   max_measurement_id
--                    highest measurement_id folded into the row; the table
--                    watermark is MAX(max_measurement_id)
--
-- Pattern:         One-time bootstrap (runs only when the table does not exist);
--                  every later run uses ../update/all_gigameter_school_measurement_stats.sql
--                  (delta-combine MERGE on school_id_giga).
--
-- Dependencies:    default.all_gigameter_measurement_data (incremental Step 4)
-- ==============================================================================

CREATE TABLE delta_lake.default.all_gigameter_school_measurement_stats
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_school_measurement_stats'
)
AS (
    WITH measurements AS (
        SELECT
            school_id_giga,
            measurement_id,
            local_created_timestamp,
            device_id,
            rt_source,
            isp_name,
            isp_asn,
            detected_server,
            app_version,
            CASE WHEN rt_source = 'GigaMeter' AND app_version IS NOT NULL
                 THEN COALESCE(TRY_CAST(SPLIT_PART(app_version, '.', 1) AS BIGINT), 0) * 1000000
                    + COALESCE(TRY_CAST(SPLIT_PART(app_version, '.', 2) AS BIGINT), 0) * 1000
                    + COALESCE(TRY_CAST(SPLIT_PART(app_version, '.', 3) AS BIGINT), 0)
            END AS app_version_rank
        FROM delta_lake.default.all_gigameter_measurement_data
        WHERE school_id_giga IS NOT NULL
    )

    SELECT
        school_id_giga,
        MIN(local_created_timestamp) AS first_measurement_date,
        MAX(local_created_timestamp) AS last_measurement_date,
        MAX_BY(app_version, app_version_rank) AS max_app_version_gigameter,
        MAX(app_version_rank) AS max_app_version_gigameter_rank,
        MAP_KEYS(HISTOGRAM(device_id)) AS device_ids,               -- distinct non-null ids (set_agg not available on our Trino)
        HISTOGRAM(rt_source) AS source_counts,
        HISTOGRAM(isp_name) AS isp_counts,
        HISTOGRAM(isp_asn) AS isp_asn_counts,
        HISTOGRAM(detected_server) AS server_counts,
        MAX(measurement_id) AS max_measurement_id
    FROM measurements
    GROUP BY school_id_giga
)
