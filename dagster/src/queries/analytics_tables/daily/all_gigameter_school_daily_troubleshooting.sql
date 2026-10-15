

-- all_gigameter_school_daily_troubleshooting
-- Purpose: pre-aggregated school+day summary feeding the Superset "T0 troubleshooting
-- table" (dashboard.giga.global, dashboard 21, dataset 110).
--
-- One row per school per weekday. Full rebuild: drop and recreate (small table —
-- schools x working days, same shape as all_gigameter_measurement_data_daily).

-- Rebuild default.all_gigameter_school_daily_troubleshooting with:
--   1. corrected location_flagged_count (detected_location_distance > 500,
--      not the wrong detected_location_is_flagged = true)
--   2. new location_checked_count column (denominator for T0's out-of-boundary %)
--
DROP TABLE IF EXISTS default.all_gigameter_school_daily_troubleshooting;

CREATE TABLE default.all_gigameter_school_daily_troubleshooting 
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_school_daily_troubleshooting',
    partitioned_by = ARRAY['country']
)
AS (
    SELECT
        school_id_giga,
        country,
        date,

        COUNT(*) AS measurement_count,

        MIN(
            CASE WHEN app_version IS NOT NULL AND app_version <> ''
                  AND REGEXP_LIKE(app_version, '^[0-9]+\.[0-9]+\.[0-9]+$')
                 THEN CAST(SPLIT_PART(app_version, '.', 1) AS INTEGER) * 1000000
                    + CAST(SPLIT_PART(app_version, '.', 2) AS INTEGER) * 1000
                    + CAST(SPLIT_PART(app_version, '.', 3) AS INTEGER)
            END
        ) AS min_app_version_numeric,

        MIN_BY(
            app_version,
            CASE WHEN app_version IS NOT NULL AND app_version <> ''
                  AND REGEXP_LIKE(app_version, '^[0-9]+\.[0-9]+\.[0-9]+$')
                 THEN CAST(SPLIT_PART(app_version, '.', 1) AS INTEGER) * 1000000
                    + CAST(SPLIT_PART(app_version, '.', 2) AS INTEGER) * 1000
                    + CAST(SPLIT_PART(app_version, '.', 3) AS INTEGER)
            END
        ) AS min_app_version_str,

        MAX(
            CASE WHEN app_version IS NOT NULL AND app_version <> ''
                  AND REGEXP_LIKE(app_version, '^[0-9]+\.[0-9]+\.[0-9]+$')
                 THEN CAST(SPLIT_PART(app_version, '.', 1) AS INTEGER) * 1000000
                    + CAST(SPLIT_PART(app_version, '.', 2) AS INTEGER) * 1000
                    + CAST(SPLIT_PART(app_version, '.', 3) AS INTEGER)
            END
        ) AS max_app_version_numeric,

        MAX_BY(
            app_version,
            CASE WHEN app_version IS NOT NULL AND app_version <> ''
                  AND REGEXP_LIKE(app_version, '^[0-9]+\.[0-9]+\.[0-9]+$')
                 THEN CAST(SPLIT_PART(app_version, '.', 1) AS INTEGER) * 1000000
                    + CAST(SPLIT_PART(app_version, '.', 2) AS INTEGER) * 1000
                    + CAST(SPLIT_PART(app_version, '.', 3) AS INTEGER)
            END
        ) AS max_app_version_str,

        SUM(CASE WHEN detected_location_distance > 500 AND  detected_location_accuracy < 200 THEN 1 ELSE 0 END) AS location_flagged_count,
        SUM(CASE WHEN detected_location_is_flagged IS NOT NULL THEN 1 ELSE 0 END) AS location_checked_count,
        MAX(detected_location_distance) AS max_location_distance_m

    FROM default.all_gigameter_measurement_data
    --WHERE day_of_week(date) BETWEEN 1 AND 5
    GROUP BY school_id_giga, country, date
);
