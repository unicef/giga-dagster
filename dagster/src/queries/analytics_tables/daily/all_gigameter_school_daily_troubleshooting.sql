-- all_gigameter_school_daily_troubleshooting
-- Purpose: pre-aggregated school+day summary feeding the Superset "T0 troubleshooting
-- table" (dashboard.giga.global, dashboard 21, dataset 110). T0 currently runs 7
-- separate CTEs, each independently scanning + filtering the full (unpartitioned,
-- ~8.5M row) all_gigameter_measurement_data_incremental table on every dashboard
-- render — this table lets T0 read from ~2.5M pre-aggregated rows instead, once.
--
-- One row per school per weekday. Full rebuild: drop and recreate (small table —
-- schools x working days, same shape as all_gigameter_measurement_data_daily).
--
-- Deliberately separate from the in-progress device-grain all_gigameter_measurement_
-- data_daily/_weekly rework (still "Testing" status, direction not yet locked) —
-- this table is scoped narrowly to T0's needs and does not depend on that work.
-- Sources from all_gigameter_measurement_data_incremental (not the plain
-- all_gigameter_measurement_data table), matching T0's existing dependency — the
-- incremental table has the geolocation columns (detected_location_is_flagged,
-- detected_location_distance) that the plain daily/weekly tables' source lacks.
--
-- Column notes:
--   min/max_app_version_numeric: semantic version encoded as int (e.g. "2.0.3" ->
--     2000003) via the same SPLIT_PART formula T0 already uses, wrapped in a CASE
--     (not a FILTER clause) so invalid strings become NULL before any CAST is
--     attempted. T0's own CARDINALITY(SPLIT(...))=3 check only counts dot-separated
--     parts, not that they're numeric -- some MLab-sourced rows have a non-numeric
--     segment (e.g. "2.0.mlab") that still passes that check. A FILTER clause was
--     tried first here and still errored (Trino evaluates the aggregate's argument
--     expression before applying FILTER, at least for MIN_BY/MAX_BY's sort-key
--     argument), hence the CASE-based rewrite -- confirmed clean across the full
--     unfiltered 8.46M-row table before use here.
--   min/max_app_version_str: the actual version string tied to that min/max via
--     MIN_BY/MAX_BY, for display.
--   location_flagged_count / max_location_distance_m: from detected_location_
--     is_flagged / detected_location_distance (both NULL pre-app-v2.0.3).

DROP TABLE IF EXISTS default.all_gigameter_school_daily_troubleshooting;

CREATE TABLE default.all_gigameter_school_daily_troubleshooting
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_school_daily_troubleshooting'
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

        SUM(CASE WHEN detected_location_is_flagged = true THEN 1 ELSE 0 END) AS location_flagged_count,
        MAX(detected_location_distance) AS max_location_distance_m

    FROM default.all_gigameter_measurement_data_incremental
    WHERE day_of_week(date) BETWEEN 1 AND 5
    GROUP BY school_id_giga, country, date
);
