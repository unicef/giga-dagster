-- ==============================================================================
-- Script Name:     superset_activity_logs.sql
-- Table Created:   default.superset_activity_logs
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Per-event Superset activity log, enriched with the acting user's role(s),
--   access role(s), org (derived from email domain), and the dashboard/chart
--   the event touched (including dashboards only referenced via the event's
--   JSON payload, not the FK columns). Feeds Superset usage/adoption
--   dashboards.
--
-- Dependencies:
--   - superset.public.logs, ab_user, ab_user_role, ab_role, dashboards, slices
--     (Superset's own Postgres-backed metadata catalog)
--
-- Output Columns:  28 columns
-- Primary Key:     id (one row per log event)
-- Granularity:     One row per Superset log event, most recent first
--
-- Last Updated:    2026-09-04 / Luke Stringer
-- ==============================================================================

DROP TABLE IF EXISTS default.superset_activity_logs;

CREATE TABLE default.superset_activity_logs
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/superset_activity_logs'
)
AS (

SELECT
    l.id,
    l.dttm AS log_time,
    l.duration_ms,
    l.action,

    CASE
        WHEN l.action = 'log' THEN TRUE
        ELSE FALSE
    END AS log_action,

    l.user_id,
    substr(u.email, 1, 7) AS username,
    u.email AS user_email,

    (
        SELECT array_join(
            array_agg(ar.name ORDER BY ar.name),
            ', '
        )
        FROM superset.public.ab_user_role aur
        JOIN superset.public.ab_role ar
            ON aur.role_id = ar.id
        WHERE aur.user_id = u.id
          AND ar.name IN ('Admin', 'Alpha', 'Gamma', 'sql_lab', 'Public')
    ) AS user_role,

    (
        SELECT array_join(
            array_agg(ar.name ORDER BY ar.name),
            ', '
        )
        FROM superset.public.ab_user_role aur
        JOIN superset.public.ab_role ar
            ON aur.role_id = ar.id
        WHERE aur.user_id = u.id
          AND ar.name NOT IN ('Admin', 'Alpha', 'Gamma', 'sql_lab', 'Public')
    ) AS access_roles,

    COALESCE(l.dashboard_id, dd.id) AS dashboard_id,
    COALESCE(d.dashboard_title, dd.dashboard_title) AS dashboard_title,
    COALESCE(d.published, dd.published) AS dashboard_published,
    COALESCE(d.created_by_fk, dd.created_by_fk) AS dashboard_created_by_user_id,

    ud.username AS dashboard_created_by_username,
    ud.email AS dashboard_created_by_user_email,

    COALESCE(d.created_on, dd.created_on) AS dashboard_created_on,

    l.slice_id,
    s.slice_name,
    s.datasource_name,
    s.datasource_id,
    s.schema_perm,
    s.perm,

    l.json,

    json_extract_scalar(json_parse(l.json), '$.source') AS log_action_source,

    json_extract_scalar(json_parse(l.json), '$.source_id') AS log_action_source_id,

    CASE
        WHEN lower(u.email) LIKE '%@unicef.org' THEN 'unicef'
        WHEN lower(u.email) LIKE '%@itu.%' THEN 'itu'
        WHEN lower(u.email) LIKE '%@edu%' THEN 'external'
        WHEN lower(u.email) LIKE '%.gov.%' THEN 'external'
        WHEN lower(u.email) LIKE '%thinkingmachin%' THEN 'thinkingmachine'
        WHEN lower(u.email) LIKE '%@nagarro%' THEN 'nagarro'
        ELSE 'unknown'
    END AS user_org,

    CASE
        WHEN COALESCE(l.dashboard_id, dd.id) IS NOT NULL
         AND COALESCE(d.dashboard_title, dd.dashboard_title) IS NULL
            THEN 'deleted'

        WHEN COALESCE(d.published, dd.published) = TRUE
            THEN 'published'

        WHEN COALESCE(d.published, dd.published) = FALSE
            THEN 'draft'

        WHEN COALESCE(l.dashboard_id, dd.id) IS NULL
         AND COALESCE(d.dashboard_title, dd.dashboard_title) IS NULL
            THEN 'n.a.'

        ELSE 'unknown'
    END AS dashboard_status

FROM superset.public.logs l

LEFT JOIN superset.public.ab_user u
    ON l.user_id = u.id

LEFT JOIN superset.public.dashboards d
    ON l.dashboard_id = d.id

LEFT JOIN superset.public.slices s
    ON l.slice_id = s.id

LEFT JOIN superset.public.dashboards dd
    ON json_extract_scalar(json_parse(l.json), '$.source_id') = CAST(dd.id AS VARCHAR)
   AND json_extract_scalar(json_parse(l.json), '$.source') = 'dashboard'
   AND l.dashboard_id IS NULL

LEFT JOIN superset.public.ab_user ud
    ON COALESCE(d.created_by_fk, dd.created_by_fk) = ud.id

ORDER BY l.dttm DESC

);
