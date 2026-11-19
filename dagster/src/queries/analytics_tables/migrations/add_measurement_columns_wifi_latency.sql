-- =============================================================================
-- Migration: add Giga Meter measurement columns (TECH-11604 + TECH-11791)
--
-- RUN THIS BEFORE deploying the matching create/update SQL.
-- The incremental updates are positional INSERT INTO … SELECT (no column
-- list). If the code deploys first, the next hourly run fails on column
-- count. If this ALTER runs first with old SQL, the same happens. Both have
-- to happen in one paused window. A failed run is harmless (id watermark
-- self-heals) but avoid it.
--
-- Window (see incremental/README.md "Schema changes on incremental tables"):
--   1. After the hh:15 incremental run. Avoid 00:15–~01:30 (Spark OPTIMIZE
--      at 00:45) and ~04:00 (daily).
--   2. Pause analytics_tables_incremental_schedule (and the daily one if
--      near 04:00). Confirm nothing is running.
--   3. Run this file.
--   4. DESCRIBE each table: new columns must be LAST, in this order, and
--      Steps 1 and 2 must stay aligned.
--   5. Deploy. Unpause. Materialise one incremental run by hand.
--
-- One-off: NOT run by the pipeline (nothing in Dagster reads migrations/).
-- Run by hand in SQL Lab / Trino UI with a write connection (prod port-forward
-- is SELECT-only). Staging tables first, then prod.
-- ADD COLUMN is metadata-only (no data-file rewrite). Existing rows stay NULL;
-- any backfill is a separate follow-up PR.
--
-- Rollback: revert the SQL and leave the columns NULL. DROP COLUMN likely
-- needs Delta column mapping mode `name`; these tables have none
-- (minReaderVersion=3, minWriterVersion=7, timestampNtz only).
-- Do NOT DROP + recreate: the incremental runner does not delete the ADLS
-- folder, bootstrap caps take days, and the shared max-id watermark can skip
-- rows.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 1: all_gmeter_only_measurements  (5 new columns → 68 total)
-- ---------------------------------------------------------------------------
ALTER TABLE delta_lake.default.all_gmeter_only_measurements ADD COLUMN IF NOT EXISTS wifi_unavailable_reason VARCHAR;
ALTER TABLE delta_lake.default.all_gmeter_only_measurements ADD COLUMN IF NOT EXISTS ssid_source VARCHAR;
ALTER TABLE delta_lake.default.all_gmeter_only_measurements ADD COLUMN IF NOT EXISTS device_type VARCHAR;
ALTER TABLE delta_lake.default.all_gmeter_only_measurements ADD COLUMN IF NOT EXISTS device_info VARCHAR;
ALTER TABLE delta_lake.default.all_gmeter_only_measurements ADD COLUMN IF NOT EXISTS device_context VARCHAR;

DESCRIBE delta_lake.default.all_gmeter_only_measurements;

-- ---------------------------------------------------------------------------
-- Step 2: all_mlab_only_measurements  (same 5, always NULL)
-- Alter back-to-back with Step 1: three sites do m1.* UNION ALL m2.*.
-- ---------------------------------------------------------------------------
ALTER TABLE delta_lake.default.all_mlab_only_measurements ADD COLUMN IF NOT EXISTS wifi_unavailable_reason VARCHAR;
ALTER TABLE delta_lake.default.all_mlab_only_measurements ADD COLUMN IF NOT EXISTS ssid_source VARCHAR;
ALTER TABLE delta_lake.default.all_mlab_only_measurements ADD COLUMN IF NOT EXISTS device_type VARCHAR;
ALTER TABLE delta_lake.default.all_mlab_only_measurements ADD COLUMN IF NOT EXISTS device_info VARCHAR;
ALTER TABLE delta_lake.default.all_mlab_only_measurements ADD COLUMN IF NOT EXISTS device_context VARCHAR;

DESCRIBE delta_lake.default.all_mlab_only_measurements;

-- ---------------------------------------------------------------------------
-- Step 4: all_gigameter_measurement_data
-- Same 5 as Steps 1/2, then loaded + idle download latency (7 new → 85 total).
-- Upload latency and RTTVar are not added in this change.
-- ---------------------------------------------------------------------------
ALTER TABLE delta_lake.default.all_gigameter_measurement_data ADD COLUMN IF NOT EXISTS wifi_unavailable_reason VARCHAR;
ALTER TABLE delta_lake.default.all_gigameter_measurement_data ADD COLUMN IF NOT EXISTS ssid_source VARCHAR;
ALTER TABLE delta_lake.default.all_gigameter_measurement_data ADD COLUMN IF NOT EXISTS device_type VARCHAR;
ALTER TABLE delta_lake.default.all_gigameter_measurement_data ADD COLUMN IF NOT EXISTS device_info VARCHAR;
ALTER TABLE delta_lake.default.all_gigameter_measurement_data ADD COLUMN IF NOT EXISTS device_context VARCHAR;
ALTER TABLE delta_lake.default.all_gigameter_measurement_data ADD COLUMN IF NOT EXISTS loaded_latency_download_ms DOUBLE;
ALTER TABLE delta_lake.default.all_gigameter_measurement_data ADD COLUMN IF NOT EXISTS idle_latency_download_ms DOUBLE;

DESCRIBE delta_lake.default.all_gigameter_measurement_data;

-- Confirm order (last 5 of Steps 1/2 must match; last 7 of Step 4).
SELECT table_name, ordinal_position, column_name, data_type
FROM delta_lake.information_schema.columns
WHERE table_schema = 'default'
  AND table_name IN (
      'all_gmeter_only_measurements',
      'all_mlab_only_measurements',
      'all_gigameter_measurement_data'
  )
ORDER BY table_name, ordinal_position;
