-- ==============================================================================
-- Script Name:     all_gigameter_school_daily_troubleshooting.sql (incremental/update)
-- Table Updated:   default.all_gigameter_school_daily_troubleshooting
--
-- Pattern:         Delta-combine MERGE, hourly.
--   1. Aggregate only measurements with measurement_id above this table's OWN
--      watermark (MAX(max_measurement_id)) to the target grain
--      (school_id_giga, country, date).
--   2. MERGE into the existing rows, combining old and new values exactly:
--      counts/sums add, min/max compare, min_by/max_by follow the numeric
--      version. Unseen keys are inserted.
--
-- Why this pattern:
--   - measurement_id is unique and strictly increasing, so "new rows" is one
--     prunable comparison -- no trailing-window recompute or id-array
--     reconciliation needed (cf. all_ping_hourly, which has no such marker).
--   - A late / back-dated measurement updates its existing (school, date) row
--     instead of being lost (a plain append can't correct a written row).
--   - Today's partial day is always current.
--   - The watermark is written in the same commit as the data, so a failed run
--     leaves nothing half-done and the next run retries the same batch.
--
-- Notes:
--   - No batch cap: capping rows before a GROUP BY can split a group across runs.
--     Upstream Steps 1-2 already bound per-run growth.
--   - Keys are matched NULL-safely (IS NOT DISTINCT FROM): unmatched MLab rows
--     with NULL school_id_giga / country form their own row, as before; a plain
--     `=` would insert a duplicate NULL-key row on every run.
--   - In UPDATE SET every t.* refers to the OLD row values, so assignment order
--     does not matter.
--
-- Dependencies:    default.all_gigameter_measurement_data (incremental Step 4)
-- ==============================================================================

MERGE INTO delta_lake.default.all_gigameter_school_daily_troubleshooting AS t
USING (
    WITH batch AS (
        SELECT
            school_id_giga,
            country,
            date,
            measurement_id,
            app_version,
            detected_location_distance,
            detected_location_accuracy,
            detected_location_is_flagged,
            CASE WHEN app_version IS NOT NULL AND app_version <> ''
                  AND REGEXP_LIKE(app_version, '^[0-9]+\.[0-9]+\.[0-9]+$')
                 THEN CAST(SPLIT_PART(app_version, '.', 1) AS INTEGER) * 1000000
                    + CAST(SPLIT_PART(app_version, '.', 2) AS INTEGER) * 1000
                    + CAST(SPLIT_PART(app_version, '.', 3) AS INTEGER)
            END AS app_version_numeric
        FROM delta_lake.default.all_gigameter_measurement_data
        WHERE measurement_id > (
            SELECT COALESCE(MAX(max_measurement_id), 0)
            FROM delta_lake.default.all_gigameter_school_daily_troubleshooting
        )
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
    FROM batch
    GROUP BY school_id_giga, country, date
) AS s
ON  t.school_id_giga IS NOT DISTINCT FROM s.school_id_giga
AND t.country IS NOT DISTINCT FROM s.country
AND t.date = s.date

WHEN MATCHED THEN UPDATE SET
    measurement_count = t.measurement_count + s.measurement_count,
    min_app_version_numeric = LEAST(
        COALESCE(t.min_app_version_numeric, s.min_app_version_numeric),
        COALESCE(s.min_app_version_numeric, t.min_app_version_numeric)
    ),
    min_app_version_str = CASE
        WHEN s.min_app_version_numeric IS NOT NULL
         AND (t.min_app_version_numeric IS NULL OR s.min_app_version_numeric < t.min_app_version_numeric)
        THEN s.min_app_version_str
        ELSE t.min_app_version_str
    END,
    max_app_version_numeric = GREATEST(
        COALESCE(t.max_app_version_numeric, s.max_app_version_numeric),
        COALESCE(s.max_app_version_numeric, t.max_app_version_numeric)
    ),
    max_app_version_str = CASE
        WHEN s.max_app_version_numeric IS NOT NULL
         AND (t.max_app_version_numeric IS NULL OR s.max_app_version_numeric > t.max_app_version_numeric)
        THEN s.max_app_version_str
        ELSE t.max_app_version_str
    END,
    location_flagged_count = t.location_flagged_count + s.location_flagged_count,
    location_checked_count = t.location_checked_count + s.location_checked_count,
    max_location_distance_m = GREATEST(
        COALESCE(t.max_location_distance_m, s.max_location_distance_m),
        COALESCE(s.max_location_distance_m, t.max_location_distance_m)
    ),
    max_measurement_id = GREATEST(t.max_measurement_id, s.max_measurement_id)

WHEN NOT MATCHED THEN INSERT (
    school_id_giga,
    country,
    date,
    measurement_count,
    min_app_version_numeric,
    min_app_version_str,
    max_app_version_numeric,
    max_app_version_str,
    location_flagged_count,
    location_checked_count,
    max_location_distance_m,
    max_measurement_id
) VALUES (
    s.school_id_giga,
    s.country,
    s.date,
    s.measurement_count,
    s.min_app_version_numeric,
    s.min_app_version_str,
    s.max_app_version_numeric,
    s.max_app_version_str,
    s.location_flagged_count,
    s.location_checked_count,
    s.max_location_distance_m,
    s.max_measurement_id
)
