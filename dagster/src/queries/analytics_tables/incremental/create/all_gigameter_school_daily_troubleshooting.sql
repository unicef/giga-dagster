-- ==============================================================================
-- Script Name:     all_gigameter_school_daily_troubleshooting.sql (incremental/create)
-- Table Created:   default.all_gigameter_school_daily_troubleshooting
--
-- Purpose:
--   Pre-aggregated school+day summary feeding the Superset "T0 troubleshooting
--   table" (dashboard.giga.global, dashboard 21, dataset 110).
--
-- Granularity:     One row per (school_id_giga, country, date) -- all days, no
--                  weekday filter (presentation filters belong in Superset).
--                  `date` is the UTC date of the measurement. Unmatched MLab rows
--                  (NULL school_id_giga / country) are kept as their own group.
--
-- Pattern:         One-time bootstrap. Runs only when the table does not exist
--                  (see _run_incremental in assets.py); every later run uses
--                  ../update/all_gigameter_school_daily_troubleshooting.sql, a
--                  delta-combine MERGE that aggregates only measurements with
--                  measurement_id above this table's own watermark and combines
--                  them into the existing rows.
--
-- Columns:         Same output columns as the retired daily script, plus
--                  max_measurement_id (BIGINT) -- the highest measurement_id folded
--                  into the row. Internal watermark for the update script; the
--                  table watermark is MAX(max_measurement_id).
--
-- Dependencies:    default.all_gigameter_measurement_data (incremental Step 4)
-- ==============================================================================

CREATE TABLE delta_lake.default.all_gigameter_school_daily_troubleshooting
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_school_daily_troubleshooting',
    partitioned_by = ARRAY['country']
)
AS (
    WITH measurements AS (
        SELECT
            school_id_giga,
            country,
            date,
            measurement_id,
            app_version,
            detected_location_distance,
            detected_location_accuracy,
            detected_location_is_flagged,
            -- numeric version (major*1e6 + minor*1e3 + patch) for valid x.y.z strings
            CASE WHEN app_version IS NOT NULL AND app_version <> ''
                  AND REGEXP_LIKE(app_version, '^[0-9]+\.[0-9]+\.[0-9]+$')
                 THEN CAST(SPLIT_PART(app_version, '.', 1) AS INTEGER) * 1000000
                    + CAST(SPLIT_PART(app_version, '.', 2) AS INTEGER) * 1000
                    + CAST(SPLIT_PART(app_version, '.', 3) AS INTEGER)
            END AS app_version_numeric
        FROM delta_lake.default.all_gigameter_measurement_data
    )

    SELECT
        school_id_giga,
        country,
        date,
        COUNT(*) AS measurement_count,
        MIN(app_version_numeric) AS min_app_version_numeric,
        MIN_BY(app_version, app_version_numeric) AS min_app_version_str,
        MAX(app_version_numeric) AS max_app_version_numeric,
        MAX_BY(app_version, app_version_numeric) AS max_app_version_str,
        SUM(CASE WHEN detected_location_distance > 500 AND detected_location_accuracy < 200 THEN 1 ELSE 0 END) AS location_flagged_count,
        SUM(CASE WHEN detected_location_is_flagged IS NOT NULL THEN 1 ELSE 0 END) AS location_checked_count,
        MAX(detected_location_distance) AS max_location_distance_m,
        MAX(measurement_id) AS max_measurement_id
    FROM measurements
    GROUP BY school_id_giga, country, date
)
