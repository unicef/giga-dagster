
INSERT INTO delta_lake.default.all_gigameter_valid_test_checker_incremental



--STEP 3b

-- ============================================================
-- CTE 1: data
-- Combines GigaMeter and MLab measurements via UNION ALL
-- ============================================================
WITH data AS (
  SELECT m1.* FROM delta_lake.default.all_gmeter_only_measurements_incremental  m1
  -- ID FILTERING: measurement_id is strictly increasing, so this single
  -- comparison selects new rows and guarantees no duplicates -- cheaper and
  -- more scalable than NOT IN (a full anti-join against a growing table)
  WHERE m1.measurement_id > (
    SELECT COALESCE(MAX(measurement_id), 0) FROM delta_lake.default.all_gigameter_valid_test_checker_incremental
  )
  UNION ALL
  SELECT m2.* FROM delta_lake.default.all_mlab_only_measurements_incremental m2
  -- ID FILTERING
  WHERE m2.measurement_id > (
    SELECT COALESCE(MAX(measurement_id), 0) FROM delta_lake.default.all_gigameter_valid_test_checker_incremental
  )
),

-- ============================================================
-- CTE 2: flagged
-- Derives measurement_protocol, the categorical download/upload
-- flags, and the IsSmall (insufficient data) checks.
-- ============================================================
flagged AS (
  SELECT
    -- Key identifiers
    measurement_id,
    measurement_uuid,
    rt_source,
    created_timestamp,
    school_id_giga,
    school_id_govt,
    device_id,

    -- Derive measurement protocol from JSON field presence (robust to
    -- future app_version changes)
    CASE
      WHEN s2c_lastclient_elapsed_time IS NOT NULL THEN 'ndt-7'
      WHEN ndt5_c2s_rate_kbps IS NOT NULL          THEN 'ndt-5'
      ELSE 'undetermined'
    END AS measurement_protocol,

    -- Raw metrics passed through
    s2c_lastclient_elapsed_time,
    s2c_bytes_Acked,
    s2c_FinalSnapshot,
    c2s_lastclient_elapsed_time,
    c2s_bytes_Acked,
    c2s_FinalSnapshot,
    c2s_tcpinfo_elapsed_time_us,
    c2s_bytes_received,
    download_speed,
    upload_speed,
    upload_speed_legacy,
    ndt5_duration_s,
    ndt5_bytes_acked,
    ndt5_download_speed_alt,
    -- Geolocation (passed through from upstream; NULL for MLab)
    detected_latitude,
    detected_longitude,
    detected_location_is_flagged,
    detected_location_distance,
    detected_location_accuracy,
    -- ----------------------------------------------------------
    -- Download flag
    -- ----------------------------------------------------------
    CAST(CASE
      WHEN s2c_lastclient_elapsed_time IS NOT NULL THEN
        -- ndt-7
        CASE
          WHEN s2c_FinalSnapshot IS NULL THEN 'last_snapshot_unavailable'
          WHEN CAST(s2c_lastclient_elapsed_time AS DECIMAL(18,6)) BETWEEN 9 AND 60
            THEN 'pass'
          WHEN CAST(s2c_lastclient_elapsed_time AS DECIMAL(18,6)) < 9
               AND download_speed >= 200
            THEN 'early_exit'
          WHEN CAST(s2c_lastclient_elapsed_time AS DECIMAL(18,6)) < 9
            THEN 'too_short'
          WHEN CAST(s2c_lastclient_elapsed_time AS DECIMAL(18,6)) > 60
            THEN 'too_long'
          ELSE 'investigate'
        END
      WHEN ndt5_c2s_rate_kbps IS NOT NULL THEN
        -- ndt-5: shared duration field for both directions
        CASE
          WHEN ndt5_duration_s IS NULL THEN 'undetermined'
          WHEN ndt5_duration_s BETWEEN 9 AND 60
            THEN 'pass'
          WHEN ndt5_duration_s < 9 AND download_speed >= 200
            THEN 'early_exit'
          WHEN ndt5_duration_s < 9
            THEN 'too_short'
          WHEN ndt5_duration_s > 60
            THEN 'too_long'
          ELSE 'investigate'
        END
      ELSE 'undetermined'
    END AS VARCHAR) AS download_flag,

    -- ----------------------------------------------------------
    -- Upload flag
    -- ----------------------------------------------------------
    CAST(CASE
      WHEN s2c_lastclient_elapsed_time IS NOT NULL THEN
        -- ndt-7
        CASE
          WHEN c2s_FinalSnapshot IS NULL OR c2s_tcpinfo_elapsed_time_us IS NULL
            THEN 'last_snapshot_unavailable'
          WHEN c2s_tcpinfo_elapsed_time_us BETWEEN 9000000 AND 60000000
            THEN 'pass'
          WHEN c2s_tcpinfo_elapsed_time_us < 9000000 AND upload_speed >= 200
            THEN 'early_exit'
          WHEN c2s_tcpinfo_elapsed_time_us < 9000000
            THEN 'too_short'
          WHEN c2s_tcpinfo_elapsed_time_us > 60000000
            THEN 'too_long'
          ELSE 'investigate'
        END
      WHEN ndt5_c2s_rate_kbps IS NOT NULL THEN
        -- ndt-5: shared duration field for both directions
        CASE
          WHEN ndt5_duration_s IS NULL THEN 'undetermined'
          WHEN ndt5_duration_s BETWEEN 9 AND 60
            THEN 'pass'
          WHEN ndt5_duration_s < 9 AND upload_speed_legacy >= 200
            THEN 'early_exit'
          WHEN ndt5_duration_s < 9
            THEN 'too_short'
          WHEN ndt5_duration_s > 60
            THEN 'too_long'
          ELSE 'investigate'
        END
      ELSE 'undetermined'
    END AS VARCHAR) AS upload_flag,

    -- ----------------------------------------------------------
    -- IsSmall checks (insufficient data transferred)
    -- ----------------------------------------------------------
    COALESCE(CASE
      WHEN s2c_lastclient_elapsed_time IS NOT NULL THEN CAST(s2c_bytes_Acked AS BIGINT) < 8192
      WHEN ndt5_c2s_rate_kbps IS NOT NULL AND ndt5_bytes_acked IS NOT NULL THEN ndt5_bytes_acked < 8192
      ELSE FALSE
    END, FALSE) AS download_data_is_small,

    COALESCE(CASE
      WHEN s2c_lastclient_elapsed_time IS NOT NULL THEN c2s_bytes_received < 8192
      ELSE FALSE  -- ndt-5 upload: no bytes field available, not checked
    END, FALSE) AS upload_data_is_small

  FROM data

    
),

-- ============================================================
-- CTE 3: pf
-- Derives per-direction pass/fail from the categorical flags
-- and IsSmall checks.
-- ============================================================
pf AS (
  SELECT
    *,
    CAST(CASE
      WHEN download_flag = 'undetermined' THEN NULL
      WHEN download_flag IN ('pass', 'early_exit') AND NOT download_data_is_small THEN 'pass'
      ELSE 'fail'
    END AS VARCHAR) AS pass_fail_download,

    CAST(CASE
      WHEN upload_flag = 'undetermined' THEN NULL
      WHEN upload_flag IN ('pass', 'early_exit') AND NOT upload_data_is_small THEN 'pass'
      ELSE 'fail'
    END AS VARCHAR) AS pass_fail_upload
  FROM flagged
)

-- ============================================================
-- FINAL SELECT
-- Adds overall pass/fail and human-readable failure reasons.
-- ============================================================
SELECT
  measurement_id,
  measurement_uuid,
  rt_source,
  created_timestamp,
  school_id_giga,
  school_id_govt,
  device_id,
  measurement_protocol,

  -- Raw metrics
  s2c_lastclient_elapsed_time,
  s2c_bytes_Acked,
  s2c_FinalSnapshot,
  c2s_lastclient_elapsed_time,
  c2s_bytes_Acked,
  c2s_FinalSnapshot,
  c2s_tcpinfo_elapsed_time_us,
  c2s_bytes_received,
  ndt5_duration_s,
  ndt5_bytes_acked,
  ndt5_download_speed_alt,
  
  -- Geolocation (passed through from upstream; NULL for MLab)
  detected_latitude,
  detected_longitude,
  detected_location_is_flagged,
  detected_location_distance,
  detected_location_accuracy,

  -- Categorical flags (detailed classification)
  download_flag,
  upload_flag,

  -- ----------------------------------------------------------
  -- Overall pass/fail
  -- ----------------------------------------------------------
  CAST(CASE
    WHEN pass_fail_download = 'pass' AND pass_fail_upload = 'pass' THEN 'pass'
    WHEN pass_fail_download = 'fail' OR pass_fail_upload = 'fail' THEN 'fail'
    ELSE NULL  -- both undetermined, or one pass + one undetermined
  END AS VARCHAR) AS pass_fail_overall,

  -- Overall failure reasons (deduplicated across directions)
  CAST(NULLIF(CONCAT_WS(', ',
    CASE
      WHEN download_flag = 'too_short' AND upload_flag = 'too_short' THEN 'test too short (download & upload)'
      WHEN download_flag = 'too_short'                               THEN 'test too short (download)'
      WHEN upload_flag   = 'too_short'                               THEN 'test too short (upload)'
    END,
    CASE
      WHEN download_flag = 'too_long' AND upload_flag = 'too_long'   THEN 'test too long (download & upload)'
      WHEN download_flag = 'too_long'                                THEN 'test too long (download)'
      WHEN upload_flag   = 'too_long'                                THEN 'test too long (upload)'
    END,
    CASE
      WHEN download_flag = 'last_snapshot_unavailable' AND upload_flag = 'last_snapshot_unavailable'
                                                                       THEN 'test incomplete (download & upload)'
      WHEN download_flag = 'last_snapshot_unavailable'                THEN 'test incomplete (download)'
      WHEN upload_flag   = 'last_snapshot_unavailable'                THEN 'test incomplete (upload)'
    END,
    CASE
      WHEN download_data_is_small AND upload_data_is_small THEN 'insufficient data (download & upload)'
      WHEN download_data_is_small                          THEN 'insufficient data (download)'
      WHEN upload_data_is_small                            THEN 'insufficient data (upload)'
    END,
    CASE
      WHEN download_flag = 'investigate' AND upload_flag = 'investigate' THEN 'investigate (download & upload)'
      WHEN download_flag = 'investigate'                                 THEN 'investigate (download)'
      WHEN upload_flag   = 'investigate'                                 THEN 'investigate (upload)'
    END
  ), '') AS VARCHAR) AS reasons_failed_overall,

  -- ----------------------------------------------------------
  -- Download pass/fail and reasons
  -- ----------------------------------------------------------
  pass_fail_download,

  CAST(NULLIF(CONCAT_WS(', ',
    CASE WHEN download_flag = 'too_short'                THEN 'test too short (<9s)' END,
    CASE WHEN download_flag = 'too_long'                 THEN 'test too long (>60s)' END,
    CASE WHEN download_flag = 'last_snapshot_unavailable' THEN 'test incomplete' END,
    CASE WHEN download_flag = 'investigate'              THEN 'investigate' END,
    CASE WHEN download_data_is_small                     THEN 'insufficient data (<8192 bytes)' END
  ), '') AS VARCHAR) AS reasons_failed_download,

  -- ----------------------------------------------------------
  -- Upload pass/fail and reasons
  -- ----------------------------------------------------------
  pass_fail_upload,

  CAST(NULLIF(CONCAT_WS(', ',
    CASE WHEN upload_flag = 'too_short'                THEN 'test too short (<9s)' END,
    CASE WHEN upload_flag = 'too_long'                 THEN 'test too long (>60s)' END,
    CASE WHEN upload_flag = 'last_snapshot_unavailable' THEN 'test incomplete' END,
    CASE WHEN upload_flag = 'investigate'              THEN 'investigate' END,
    CASE WHEN upload_data_is_small                     THEN 'insufficient data (<8192 bytes)' END
  ), '') AS VARCHAR) AS reasons_failed_upload

FROM pf

