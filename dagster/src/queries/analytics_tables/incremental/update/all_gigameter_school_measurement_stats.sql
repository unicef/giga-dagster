-- ==============================================================================
-- Script Name:     all_gigameter_school_measurement_stats.sql (incremental/update)
-- Table Updated:   default.all_gigameter_school_measurement_stats
--
-- Pattern:         Delta-combine MERGE, hourly (same technique as
--                  update/all_gigameter_school_daily_troubleshooting.sql).
--   1. Aggregate only measurements with measurement_id above this table's OWN
--      watermark (MAX(max_measurement_id)), per non-null school_id_giga.
--   2. MERGE on school_id_giga, combining exactly:
--        first/last date    LEAST / GREATEST
--        max app version    keep the higher rank
--        device_ids         array_distinct(concat(old, new))
--        *_counts maps      per-key sum (map_zip_with)
--      Unseen schools are inserted.
--
-- Notes:
--   - No batch cap (never cap rows before a GROUP BY).
--   - Rows with NULL school_id_giga are excluded and never advance the
--     watermark, so each run re-reads the (small) unmatched-MLab tail above it
--     and drops it again. Harmless; watch for growth.
--   - Trino GREATEST/LEAST return NULL if any argument is NULL, hence the
--     COALESCE wrapping. In UPDATE SET every t.* is the OLD row value.
--
-- Dependencies:    default.all_gigameter_measurement_data (incremental Step 4)
-- ==============================================================================

MERGE INTO delta_lake.default.all_gigameter_school_measurement_stats AS t
USING (
    WITH batch AS (
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
          AND measurement_id > (
              SELECT COALESCE(MAX(max_measurement_id), 0)
              FROM delta_lake.default.all_gigameter_school_measurement_stats
          )
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
    FROM batch
    GROUP BY school_id_giga
) AS s
ON t.school_id_giga = s.school_id_giga

WHEN MATCHED THEN UPDATE SET
    first_measurement_date = LEAST(
        COALESCE(t.first_measurement_date, s.first_measurement_date),
        COALESCE(s.first_measurement_date, t.first_measurement_date)
    ),
    last_measurement_date = GREATEST(
        COALESCE(t.last_measurement_date, s.last_measurement_date),
        COALESCE(s.last_measurement_date, t.last_measurement_date)
    ),
    max_app_version_gigameter = CASE
        WHEN s.max_app_version_gigameter_rank IS NOT NULL
         AND (t.max_app_version_gigameter_rank IS NULL OR s.max_app_version_gigameter_rank > t.max_app_version_gigameter_rank)
        THEN s.max_app_version_gigameter
        ELSE t.max_app_version_gigameter
    END,
    max_app_version_gigameter_rank = GREATEST(
        COALESCE(t.max_app_version_gigameter_rank, s.max_app_version_gigameter_rank),
        COALESCE(s.max_app_version_gigameter_rank, t.max_app_version_gigameter_rank)
    ),
    -- NULL stays NULL (no device ids yet) to match create/; no typed empties needed
    device_ids = ARRAY_DISTINCT(CONCAT(
        COALESCE(t.device_ids, s.device_ids),
        COALESCE(s.device_ids, t.device_ids)
    )),
    source_counts = CASE
        WHEN t.source_counts IS NULL THEN s.source_counts
        WHEN s.source_counts IS NULL THEN t.source_counts
        ELSE MAP_ZIP_WITH(t.source_counts, s.source_counts, (k, a, b) -> COALESCE(a, 0) + COALESCE(b, 0))
    END,
    isp_counts = CASE
        WHEN t.isp_counts IS NULL THEN s.isp_counts
        WHEN s.isp_counts IS NULL THEN t.isp_counts
        ELSE MAP_ZIP_WITH(t.isp_counts, s.isp_counts, (k, a, b) -> COALESCE(a, 0) + COALESCE(b, 0))
    END,
    isp_asn_counts = CASE
        WHEN t.isp_asn_counts IS NULL THEN s.isp_asn_counts
        WHEN s.isp_asn_counts IS NULL THEN t.isp_asn_counts
        ELSE MAP_ZIP_WITH(t.isp_asn_counts, s.isp_asn_counts, (k, a, b) -> COALESCE(a, 0) + COALESCE(b, 0))
    END,
    server_counts = CASE
        WHEN t.server_counts IS NULL THEN s.server_counts
        WHEN s.server_counts IS NULL THEN t.server_counts
        ELSE MAP_ZIP_WITH(t.server_counts, s.server_counts, (k, a, b) -> COALESCE(a, 0) + COALESCE(b, 0))
    END,
    max_measurement_id = GREATEST(t.max_measurement_id, s.max_measurement_id)

WHEN NOT MATCHED THEN INSERT (
    school_id_giga,
    first_measurement_date,
    last_measurement_date,
    max_app_version_gigameter,
    max_app_version_gigameter_rank,
    device_ids,
    source_counts,
    isp_counts,
    isp_asn_counts,
    server_counts,
    max_measurement_id
) VALUES (
    s.school_id_giga,
    s.first_measurement_date,
    s.last_measurement_date,
    s.max_app_version_gigameter,
    s.max_app_version_gigameter_rank,
    s.device_ids,
    s.source_counts,
    s.isp_counts,
    s.isp_asn_counts,
    s.server_counts,
    s.max_measurement_id
)
