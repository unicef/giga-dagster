




-- ==============================================================================
-- Script Name:     bra_benchmarkstatus_wow.sql
-- Table Created:   default.bra_benchmarkstatus_wow
-- Schema:          default
-- Region:          Brazil
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Weekly snapshot of Brazil school download benchmark status
--   (good / moderate / bad / unknown) with week-over-week change metrics.
--   Uses LAG window functions to compare each week's counts and averages
--   against the prior week, enabling trend detection in Brazil school
--   connectivity performance over time.
--
-- Dependencies:
--   - default.bra_nicbr_daily (daily NIC.BR measurements with benchmark flags)
--
-- Output Columns:  ~20 columns
-- Primary Key:     week (ISO week start date, Monday)
-- Granularity:     One row per week, ordered most recent first
--
-- Run Notes:
--   Recurring — refresh weekly after bra_nicbr_daily is updated.
--   wow_change_* columns show absolute change vs the prior week;
--   wow_change_percentage_* shows percentage change vs the prior week.
--
-- Last Updated:    2025-10-31 / Luke Stringer
-- ==============================================================================


 DROP TABLE IF EXISTS default.bra_benchmarkstatus_wow;

 CREATE TABLE IF NOT EXISTS default.bra_benchmarkstatus_wow
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/bra_benchmarkstatus_wow'
)
AS (

WITH
  vt AS (
   SELECT *
   FROM
     default.bra_nicbr_daily
   ORDER BY date ASC
)
, weekly_data_per_school AS (
   SELECT
     school_id_giga
   , DATE_TRUNC('week', date) week_start
   , avg(speed_download_mean) speed_download_mean
   , avg(download_speed_benchmark) download_speed_benchmark
   , (CASE WHEN (avg(speed_download_mean) >= avg(download_speed_benchmark)) THEN 'good' WHEN ((avg(speed_download_mean) < avg(download_speed_benchmark)) AND (avg(speed_download_mean) >= 1)) THEN 'moderate' WHEN (avg(speed_download_mean) < 1) THEN 'bad' ELSE 'unknown' END) rtm_weekly_status
   , (avg(speed_download_mean) / nullif(avg(num_students), 0)) mbps_per_student
   FROM
     vt
   GROUP BY 1, 2
)
, weekly_data AS (
   SELECT
     week_start
   , COUNT(DISTINCT school_id_giga) cw_total_schools_count
   , COUNT(DISTINCT school_id_giga) FILTER (WHERE (rtm_weekly_status = 'good')) cw_good_count
   , COUNT(DISTINCT school_id_giga) FILTER (WHERE (rtm_weekly_status = 'moderate')) cw_moderate_count
   , COUNT(DISTINCT school_id_giga) FILTER (WHERE (rtm_weekly_status = 'bad')) cw_bad_count
   , COUNT(DISTINCT school_id_giga) FILTER (WHERE (rtm_weekly_status = 'unknown')) cw_unknown_count
   , COUNT(DISTINCT school_id_giga) FILTER (WHERE (mbps_per_student >= 1)) cw_above_1_mbps_per_student_count
   , avg(mbps_per_student) cw_avg_mbps_per_student
   FROM
     weekly_data_per_school
   GROUP BY 1
)
, previous_week AS (
   SELECT
     week_start
   , cw_total_schools_count
   , cw_good_count
   , LAG(cw_good_count) OVER (ORDER BY week_start ASC) pw_good_count
   , cw_moderate_count
   , LAG(cw_moderate_count) OVER (ORDER BY week_start ASC) pw_moderate_count
   , cw_bad_count
   , LAG(cw_bad_count) OVER (ORDER BY week_start ASC) pw_bad_count
   , cw_unknown_count
   , LAG(cw_unknown_count) OVER (ORDER BY week_start ASC) pw_unknown_count
   , cw_above_1_mbps_per_student_count
   , LAG(cw_above_1_mbps_per_student_count) OVER (ORDER BY week_start ASC) pw_above_1_mbps_per_student_count
   , cw_avg_mbps_per_student
   , LAG(cw_avg_mbps_per_student) OVER (ORDER BY week_start ASC) pw_avg_mbps_per_student
   FROM
     weekly_data
)
SELECT
  CAST(week_start AS DATE) AS week_start
, CAST(cw_total_schools_count AS BIGINT) AS cw_total_schools_count
, CAST(cw_good_count AS BIGINT) AS cw_good_count
, CAST(cw_moderate_count AS BIGINT) AS cw_moderate_count
, CAST(cw_bad_count AS BIGINT) AS cw_bad_count
, CAST(cw_unknown_count AS BIGINT) AS cw_unknown_count
, CAST(cw_above_1_mbps_per_student_count AS BIGINT) AS cw_above_1_mbps_per_student_count
, CAST(cw_avg_mbps_per_student AS DOUBLE) AS cw_avg_mbps_per_student
, CAST(pw_good_count AS BIGINT) AS pw_good_count
, CAST(pw_moderate_count AS BIGINT) AS pw_moderate_count
, CAST(pw_bad_count AS BIGINT) AS pw_bad_count
, CAST(pw_unknown_count AS BIGINT) AS pw_unknown_count
, CAST(pw_above_1_mbps_per_student_count AS BIGINT) AS pw_above_1_mbps_per_student_count
, CAST(pw_avg_mbps_per_student AS DOUBLE) AS pw_avg_mbps_per_student
, CAST((cw_good_count - pw_good_count) AS BIGINT) AS wow_change_good
, CAST((cw_moderate_count - pw_moderate_count) AS BIGINT) AS wow_change_moderate
, CAST((cw_bad_count - pw_bad_count) AS BIGINT) AS wow_change_bad
, CAST((CASE WHEN (pw_good_count IS NULL) THEN null ELSE ((cw_good_count - pw_good_count) / CAST(NULLIF(pw_good_count, 0) AS decimal(10, 2))) END) AS DOUBLE) AS wow_change_percentage_good
, CAST((CASE WHEN (pw_moderate_count IS NULL) THEN null ELSE ((cw_moderate_count - pw_moderate_count) / CAST(NULLIF(pw_moderate_count, 0) AS decimal(10, 2))) END) AS DOUBLE) AS wow_change_percentage_moderate
, CAST((CASE WHEN (pw_bad_count IS NULL) THEN null ELSE ((cw_bad_count - pw_bad_count) / CAST(NULLIF(pw_bad_count, 0) AS decimal(10, 2))) END) AS DOUBLE) AS wow_change_percentage_bad
, CAST((CASE WHEN (pw_unknown_count IS NULL) THEN null ELSE ((cw_unknown_count - pw_unknown_count) / CAST(NULLIF(pw_unknown_count, 0) AS decimal(10, 2))) END) AS DOUBLE) AS wow_change_percentage_unknown
, CAST((CASE WHEN (pw_above_1_mbps_per_student_count IS NULL) THEN null ELSE ((cw_above_1_mbps_per_student_count - pw_above_1_mbps_per_student_count) / CAST(NULLIF(pw_above_1_mbps_per_student_count, 0) AS decimal(10, 2))) END) AS DOUBLE) AS wow_above_1_mbps_per_student
, CAST((CASE WHEN (pw_avg_mbps_per_student IS NULL) THEN null ELSE ((cw_avg_mbps_per_student - pw_avg_mbps_per_student) / CAST(NULLIF(pw_avg_mbps_per_student, 0) AS decimal(10, 2))) END) AS DOUBLE) AS wow_avg_mbps_per_student
FROM
  previous_week
ORDER BY week_start DESC
 )
