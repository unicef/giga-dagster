-- ==============================================================================
-- Script Name:     superset_session_lengths.sql
-- Table Created:   default.superset_session_lengths
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Derives Superset user sessions from the raw activity log by gap-splitting
--   each user's events on a 30-minute idle threshold, then summarizing each
--   session's start/end, event count, and duration in minutes. Feeds Superset
--   usage/engagement dashboards.
--
-- Dependencies:
--   - superset.public.logs, ab_user
--     (Superset's own Postgres-backed metadata catalog)
--
-- Output Columns:  8 columns
-- Primary Key:     (user_id, session_start)
-- Granularity:     One row per user session
--
-- Last Updated:    2026-09-04 / Luke Stringer
-- ==============================================================================

DROP TABLE IF EXISTS default.superset_session_lengths;

CREATE TABLE default.superset_session_lengths
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/superset_session_lengths'
)
AS (

WITH ev2 AS (
    SELECT
        l.user_id
        , u.email AS user_email
        , CASE
            WHEN lower(u.email) LIKE '%@unicef.org' THEN 'unicef'
            WHEN lower(u.email) LIKE '%@itu.%' THEN 'itu'
            WHEN lower(u.email) LIKE '%@edu%' THEN 'external'
            WHEN lower(u.email) LIKE '%.gov.%' THEN 'external'
            WHEN lower(u.email) LIKE '%thinkingmachin%' THEN 'thinkingmachine'
            WHEN lower(u.email) LIKE '%@nagarro%' THEN 'nagarro'
            ELSE 'unknown'
          END AS user_org
        , l.dttm AS log_time
        , LAG(l.dttm) OVER (PARTITION BY l.user_id ORDER BY l.dttm) AS prev_t
    FROM superset.public.logs l
    LEFT JOIN superset.public.ab_user u ON l.user_id = u.id
    WHERE l.user_id IS NOT NULL
)
, flagged AS (
    SELECT
        user_id
        , user_email
        , user_org
        , log_time
        , CASE
            WHEN prev_t IS NULL OR log_time - prev_t > INTERVAL '30' MINUTE
            THEN 1 ELSE 0
          END AS new_session
    FROM ev2
)
, sessions AS (
    SELECT
        user_id
        , user_email
        , user_org
        , log_time
        , SUM(new_session) OVER (
            PARTITION BY user_id
            ORDER BY log_time
            ROWS UNBOUNDED PRECEDING
          ) AS session_seq
    FROM flagged
)
, spans AS (
    SELECT
        user_id
        , user_email
        , user_org
        , session_seq
        , MIN(log_time) AS session_start
        , MAX(log_time) AS session_end
        , COUNT(*) AS events
        , date_diff('second', MIN(log_time), MAX(log_time)) AS session_seconds
    FROM sessions
    GROUP BY 1, 2, 3, 4
)
SELECT
    user_id
    , user_email
    , user_org
    , session_start
    , CAST(session_start AS date) AS day
    , session_end
    , events
    , CAST(ROUND(session_seconds / 60.0, 1) AS DECIMAL(22, 1)) AS session_minutes
FROM spans
ORDER BY session_start

);
